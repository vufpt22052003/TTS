"""
Job Service - Orchestrates video processing jobs.

Coordinates between:
- Crawler Service (video download)
- Media Service (processing)
"""

import os
import uuid
import logging
from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MEDIA_DIR = Path(os.getenv("MEDIA_DIR", "/app/data"))
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

MEDIA_SERVICE_URL = os.getenv("MEDIA_SERVICE_URL", "http://localhost:8001")
CRAWLER_SERVICE_URL = os.getenv("CRAWLER_SERVICE_URL", "http://localhost:8002")

app = FastAPI(title="Job Service", version="1.0.0")

# In-memory job storage (use Redis/DB in production)
jobs: Dict[str, Dict[str, Any]] = {}


class JobStatus(str, Enum):
    """Job status enumeration."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ProcessOptions(BaseModel):
    """Options for video processing (passed to media-service /process)."""
    add_subtitles: bool = True
    add_voiceover: bool = True
    source_lang: Optional[str] = "auto"
    target_lang: str = "vi"
    voice: Optional[str] = None
    voiceover_volume: float = 0.8


class CreateJobRequest(BaseModel):
    """Request to create a new job."""
    url: Optional[str] = None
    video_path: Optional[str] = None
    job_id: Optional[str] = None
    options: ProcessOptions = ProcessOptions()
    # When True, block until crawler + media pipeline finish (for webhooks / simple clients).
    wait: bool = False


class JobResponse(BaseModel):
    """Job information response."""
    model_config = ConfigDict(extra="ignore")

    job_id: str
    status: JobStatus
    video_path: Optional[str] = None
    subtitle_path: Optional[str] = None
    audio_path: Optional[str] = None
    output_path: Optional[str] = None
    error: Optional[str] = None
    created_at: str
    updated_at: str
    media_job_id: Optional[str] = None
    step: Optional[str] = None


def update_job(job_id: str, **kwargs) -> None:
    """Update job information."""
    if job_id in jobs:
        jobs[job_id].update(kwargs)
        jobs[job_id]['updated_at'] = datetime.now().isoformat()


async def process_video_task(
    job_id: str,
    video_path: str,
    options: ProcessOptions
) -> None:
    """
    Background task: upload video to media-service, then run full pipeline
    (STT → translate → TTS → render) via POST /process/{media_job_id}.
    """
    logger.info(f"Starting processing for job: {job_id}")

    try:
        update_job(job_id, status=JobStatus.PROCESSING, step="uploading_to_media")

        async with httpx.AsyncClient(timeout=3600.0) as client:
            with open(video_path, "rb") as f:
                files = {"file": ("video.mp4", f, "video/mp4")}
                resp = await client.post(
                    f"{MEDIA_SERVICE_URL}/upload",
                    files=files,
                )

            if resp.status_code != 200:
                raise Exception(f"Media upload failed: {resp.text}")

            upload_json = resp.json()
            media_job_id = upload_json.get("job_id")
            if not media_job_id:
                raise Exception("Media upload did not return job_id")

            update_job(job_id, step="processing_on_media", media_job_id=media_job_id)

            process_body = {
                "source_lang": options.source_lang or "auto",
                "target_lang": options.target_lang,
                "voice": options.voice or "vi-VN-HoaiMyNeural",
                "add_subtitles": options.add_subtitles,
                "add_voiceover": options.add_voiceover,
                "voiceover_volume": options.voiceover_volume,
            }

            resp = await client.post(
                f"{MEDIA_SERVICE_URL}/process/{media_job_id}",
                json=process_body,
            )

            if resp.status_code != 200:
                raise Exception(f"Media process failed: {resp.text}")

            result = resp.json()
            if result.get("status") == "completed":
                update_job(
                    job_id,
                    status=JobStatus.COMPLETED,
                    output_path=result.get("output_path"),
                    step="completed",
                )
                logger.info(f"Job {job_id} completed successfully (media_job={media_job_id})")
            else:
                err = result.get("error") or result.get("detail") or str(result)
                raise Exception(err)

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        update_job(job_id, status=JobStatus.FAILED, error=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "job"}


@app.post("/jobs", response_model=JobResponse)
async def create_job(request: CreateJobRequest, background_tasks: BackgroundTasks):
    """
    Create a new processing job.

    Can accept either:
    - URL to download video
    - Path to local video file
    """
    job_id = request.job_id or str(uuid.uuid4())[:12]

    # Validate input
    if not request.url and not request.video_path:
        raise HTTPException(
            status_code=400,
            detail="Either 'url' or 'video_path' is required"
        )

    # Create job entry
    jobs[job_id] = {
        'job_id': job_id,
        'status': JobStatus.PENDING,
        'video_path': None,
        'subtitle_path': None,
        'audio_path': None,
        'output_path': None,
        'error': None,
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat(),
        'step': 'initializing'
    }

    video_path = None

    # Download or validate video
    if request.url:
        update_job(job_id, status=JobStatus.DOWNLOADING, step="downloading")

        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.post(
                f"{CRAWLER_SERVICE_URL}/download",
                json={'url': request.url, 'job_id': job_id}
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Download failed: {resp.text}"
                )

            result = resp.json()

            if not result.get('success'):
                raise HTTPException(
                    status_code=502,
                    detail=f"Download failed: {result.get('error')}"
                )

            video_path = result['video_path']

    elif request.video_path:
        video_path = request.video_path

    # Validate video exists
    if not video_path or not Path(video_path).exists():
        raise HTTPException(status_code=400, detail="Video file not found")

    # Update job with video path
    update_job(job_id, video_path=video_path)

    if request.wait:
        await process_video_task(job_id, video_path, request.options)
    else:
        background_tasks.add_task(
            process_video_task,
            job_id,
            video_path,
            request.options
        )

    return JobResponse(**jobs[job_id])


def _parse_wait_flag(value: str) -> bool:
    return str(value).lower() in ("true", "1", "yes", "on")


@app.post("/jobs/upload", response_model=JobResponse)
async def create_job_from_upload(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    wait: str = Form("true"),
    source_lang: str = Form("zh"),
    target_lang: str = Form("vi"),
    voice: Optional[str] = Form(None),
    add_subtitles: str = Form("true"),
    add_voiceover: str = Form("true"),
    voiceover_volume: str = Form("0.8"),
):
    """
    Upload a video file (e.g. Douyin file already downloaded via curl) and run
    the same media pipeline as POST /jobs (STT, translate, TTS, burn subtitles).

    Use multipart/form-data: field \"file\" = video; other fields optional.
    Set wait=true to block until processing finishes (default).
    """
    job_id = str(uuid.uuid4())[:12]

    jobs[job_id] = {
        "job_id": job_id,
        "status": JobStatus.PENDING,
        "video_path": None,
        "subtitle_path": None,
        "audio_path": None,
        "output_path": None,
        "error": None,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "step": "saving_upload",
    }

    job_dir = MEDIA_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    dest_path = job_dir / "upload.mp4"

    try:
        raw = await file.read()
        if not raw:
            del jobs[job_id]
            raise HTTPException(status_code=400, detail="Empty upload")
        dest_path.write_bytes(raw)
    except HTTPException:
        raise
    except Exception as e:
        if job_id in jobs:
            del jobs[job_id]
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {e}")

    video_path = str(dest_path)
    update_job(job_id, video_path=video_path)

    try:
        vol = float(voiceover_volume)
    except ValueError:
        vol = 0.8

    options = ProcessOptions(
        source_lang=source_lang or "auto",
        target_lang=target_lang,
        voice=(voice.strip() if voice and voice.strip() else None),
        add_subtitles=_parse_wait_flag(add_subtitles),
        add_voiceover=_parse_wait_flag(add_voiceover),
        voiceover_volume=vol,
    )

    request_wait = _parse_wait_flag(wait)

    if request_wait:
        await process_video_task(job_id, video_path, options)
    else:
        background_tasks.add_task(
            process_video_task,
            job_id,
            video_path,
            options,
        )

    return JobResponse(**jobs[job_id])


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    """Get job information."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse(**jobs[job_id])


@app.get("/jobs/{job_id}/result")
async def get_job_result(job_id: str):
    """Get job result (output video)."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]

    if job['status'] != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job not completed. Status: {job['status']}"
        )

    output_path = job.get('output_path')

    if not output_path or not Path(output_path).exists():
        raise HTTPException(status_code=404, detail="Output file not found")

    return FileResponse(
        path=output_path,
        filename=Path(output_path).name,
        media_type='video/mp4'
    )


@app.get("/jobs/{job_id}/subtitles")
async def get_job_subtitles(job_id: str):
    """Get subtitle file."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]
    subtitle_path = job.get('subtitle_path')

    if not subtitle_path or not Path(subtitle_path).exists():
        raise HTTPException(status_code=404, detail="Subtitle file not found")

    return FileResponse(
        path=subtitle_path,
        filename=Path(subtitle_path).name,
        media_type='text/vtt'
    )


@app.get("/jobs")
async def list_jobs(status: Optional[JobStatus] = None, limit: int = 50):
    """List all jobs."""
    result = list(jobs.values())

    if status:
        result = [j for j in result if j['status'] == status]

    # Sort by created_at descending
    result.sort(key=lambda x: x['created_at'], reverse=True)

    return result[:limit]


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its files."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    # Clean up files
    job = jobs[job_id]
    for key in ['video_path', 'output_path', 'subtitle_path', 'audio_path']:
        path = job.get(key)
        if path and Path(path).exists():
            try:
                Path(path).unlink()
            except Exception as e:
                logger.warning(f"Failed to delete {path}: {e}")

    # Remove job
    del jobs[job_id]

    return {"message": f"Job {job_id} deleted"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
