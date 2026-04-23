"""
Pipeline Processor - Orchestrates full video processing.

Coordinates: STT → Translate → TTS → Render
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Dict, Any

from app.stt import STTService, SubtitleSegment
from app.translate import TranslateService, TranslatedSegment
from app.tts_vieneu import TTSService, TTSSegment
from app.render import RenderService, RenderResult
from app.utils import ensure_dir, generate_job_id

logger = logging.getLogger(__name__)


class PipelineStep(str, Enum):
    """Pipeline step enumeration."""
    INIT = "initializing"
    EXTRACT_AUDIO = "extracting_audio"
    TRANSCRIBE = "transcribing"
    TRANSLATE = "translating"
    GENERATE_TTS = "generating_tts"
    SYNC_AUDIO = "syncing_audio"
    RENDER = "rendering"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PipelineConfig:
    """Configuration for pipeline processing."""
    source_lang: Optional[str] = None
    target_lang: str = "vi"
    voice: Optional[str] = "vi-VN-HoaiMyNeural"
    add_subtitles: bool = True
    add_voiceover: bool = True
    voiceover_volume: float = 0.8


@dataclass
class PipelineResult:
    """Result of pipeline processing."""
    success: bool
    job_id: str
    video_path: Optional[Path] = None
    audio_path: Optional[Path] = None
    subtitle_path: Optional[Path] = None
    translated_path: Optional[Path] = None
    voiceover_path: Optional[Path] = None
    output_path: Optional[Path] = None
    step: PipelineStep = PipelineStep.INIT
    error: Optional[str] = None
    warnings: List[str] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'success': self.success,
            'job_id': self.job_id,
            'video_path': str(self.video_path) if self.video_path else None,
            'audio_path': str(self.audio_path) if self.audio_path else None,
            'subtitle_path': str(self.subtitle_path) if self.subtitle_path else None,
            'translated_path': str(self.translated_path) if self.translated_path else None,
            'voiceover_path': str(self.voiceover_path) if self.voiceover_path else None,
            'output_path': str(self.output_path) if self.output_path else None,
            'step': self.step.value,
            'error': self.error,
            'warnings': self.warnings
        }


class PipelineProcessor:
    """
    Full pipeline processor.

    Pipeline:
    1. Extract audio from video
    2. Transcribe → SRT
    3. Translate → Vietnamese SRT
    4. Generate TTS
    5. Sync audio with timing
    6. Render final video
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.stt_service = STTService.get_instance()
        self.translate_service = TranslateService()
        self.tts_service = TTSService(voice=self.config.voice)
        self.render_service = RenderService()
        self._detected_lang: Optional[str] = None

    def extract_audio(self, video_path: Path, output_path: Path) -> bool:
        """Extract audio from video."""
        self._update_step(PipelineStep.EXTRACT_AUDIO)
        logger.info(f"Extracting audio from: {video_path}")
        return self.render_service.extract_audio(video_path, output_path)

    def transcribe(self, audio_path: Path) -> List[SubtitleSegment]:
        """Transcribe audio to subtitles."""
        self._update_step(PipelineStep.TRANSCRIBE)
        logger.info(f"Transcribing: {audio_path}")
        segments, lang = self.stt_service.transcribe(audio_path, language=self.config.source_lang)
        self._detected_lang = lang  # Store for translate step
        logger.info(f"Transcribed {len(segments)} segments, detected lang: {lang}")
        return segments

    def translate(self, segments: List[SubtitleSegment]) -> List[TranslatedSegment]:
        """Translate subtitles to Vietnamese."""
        self._update_step(PipelineStep.TRANSLATE)
        # Use detected language from Whisper, not config
        source_lang = self._detected_lang or self.config.source_lang or 'en'
        logger.info(f"Translating {len(segments)} segments from {source_lang}")
        translated = self.translate_service.translate_segments(
            segments,
            source_lang=source_lang
        )
        logger.info(f"Translated {len(translated)} segments")
        return translated

    def generate_tts(
        self,
        segments: List[TranslatedSegment],
        output_dir: Path
    ) -> List[TTSSegment]:
        """Generate TTS for translated segments."""
        self._update_step(PipelineStep.GENERATE_TTS)
        print(f"[PIPELINE] TTS: Input segments = {len(segments)}")
        print(f"[PIPELINE] TTS config: voice={self.config.voice}, target_lang={self.config.target_lang}")
        logger.info(f"Generating TTS for {len(segments)} segments")
        ensure_dir(output_dir)

        try:
            tts_segments = self.tts_service.generate_segments(
                segments,
                output_dir,
                voice=self.config.voice,
                language=self.config.target_lang
            )
            success_count = sum(1 for s in tts_segments if not s.error)
            print(f"[PIPELINE] TTS: Output segments = {success_count}/{len(tts_segments)}")
            
            # FAIL FAST: No valid TTS
            if success_count == 0:
                raise Exception("TTS COMPLETELY FAILED - no valid audio")
            
            return tts_segments
        except Exception as e:
            print(f"[PIPELINE] TTS ERROR: {e}")
            raise

    def sync_audio(
        self,
        tts_segments: List[TTSSegment],
        output_path: Path
    ) -> bool:
        """Sync and combine TTS segments."""
        self._update_step(PipelineStep.SYNC_AUDIO)
        print(f"[PIPELINE] SYNC: Input segments = {len(tts_segments)}")
        logger.info(f"Syncing {len(tts_segments)} audio segments")

        segment_data = []
        for seg in tts_segments:
            if seg.audio_path and Path(seg.audio_path).exists():
                original_duration = seg.end - seg.start
                print(f"[SYNC] Seg {seg.index}: start={seg.start:.3f}, tts_dur={seg.duration:.3f}")
                segment_data.append({
                    'audio_path': seg.audio_path,
                    'start': seg.start,
                    'end': seg.end,
                    'duration': seg.duration  # Use TTS duration
                })

        if not segment_data:
            raise Exception("NO VALID TTS SEGMENTS TO SYNC")

        # Check all TTS files exist before combining
        for seg in segment_data:
            if not Path(seg['audio_path']).exists():
                raise Exception(f"TTS file missing: {seg['audio_path']}")

        # combine_audio_segments will raise if fails
        result = self.render_service.combine_audio_segments(segment_data, output_path)
        
        # Verify output file
        if not output_path.exists():
            raise Exception(f"SYNC OUTPUT FILE NOT CREATED: {output_path}")
        if output_path.stat().st_size < 1000:
            raise Exception(f"SYNC OUTPUT TOO SMALL: {output_path} ({output_path.stat().st_size} bytes)")
        
        print(f"[PIPELINE] SYNC: Output = {output_path} ({output_path.stat().st_size} bytes)")
        return True

    def render(
        self,
        video_path: Path,
        subtitle_path: Path,
        voiceover_path: Optional[Path],
        output_path: Path,
        original_audio_path: Optional[Path] = None,
        voiceover_volume: float = 0.8,
        add_subtitles: bool = True
    ) -> RenderResult:
        """Render final video."""
        self._update_step(PipelineStep.RENDER)
        logger.info(f"[RENDER] add_subtitles={add_subtitles}, voiceover={voiceover_path}, volume={voiceover_volume}")
        logger.info(f"Rendering final video: {output_path}")

        return self.render_service.render_full(
            video_path=video_path,
            subtitle_path=subtitle_path,
            voiceover_path=voiceover_path,
            original_audio_path=original_audio_path,
            output_path=output_path,
            voiceover_volume=voiceover_volume,
            subtitle_track=add_subtitles
        )

    def _update_step(self, step: PipelineStep) -> None:
        """Update current step."""
        self.current_step = step
        logger.info(f"Pipeline step: {step.value}")

    def process_with_vietsub(
        self,
        video_path: Path,
        vietsub_path: Path,
        job_dir: Path,
        job_id: Optional[str] = None
    ) -> PipelineResult:
        """
        Process video using pre-uploaded vietsub file.
        Skips STT and translation - uses vietsub timing directly for TTS.

        Args:
            video_path: Input video path
            vietsub_path: Path to Vietnamese SRT file
            job_dir: Working directory for job
            job_id: Job identifier

        Returns:
            PipelineResult with all output paths
        """
        job_id = job_id or generate_job_id()
        ensure_dir(job_dir)
        self.current_step = PipelineStep.INIT

        result = PipelineResult(
            success=False,
            job_id=job_id,
            video_path=video_path
        )

        try:
            # Step 1: Parse vietsub - try dual format first (timestamp + Trung + Viet)
            from app.utils import parse_vietsub_dual_format
            self._update_step(PipelineStep.TRANSCRIBE)
            logger.info(f"Loading vietsub from: {vietsub_path}")
            
            # Try dual format first (3 lines per segment)
            dual_parsed = parse_vietsub_dual_format(vietsub_path)
            
            if dual_parsed:
                # Format: timestamp + Trung + Viet
                # Use Vietnamese text directly - NO REWRITE, NO TRANSLATION
                logger.info(f"Loaded {len(dual_parsed)} segments from dual-format vietsub")
                translated_segments = []
                for i, (seg, viet_text) in enumerate(dual_parsed, 1):
                    # Use VIETNAMESE text directly - no rewriting
                    translated_segments.append(TranslatedSegment(
                        index=i,
                        start=seg.start,
                        end=seg.end,
                        original=seg.text,  # Original Trung
                        translated=viet_text  # Vietnamese - keep EXACT
                    ))
            else:
                # Fallback: parse as normal SRT
                from app.utils import parse_srt_file
                segments = parse_srt_file(vietsub_path)
                logger.info(f"Loaded {len(segments)} segments from standard SRT")
                
                if not segments:
                    raise Exception("Vietsub file is empty or unrecognized format")
                
                # Use text as-is (no translation needed)
                translated_segments = [
                    TranslatedSegment(
                        index=i,
                        start=seg.start,
                        end=seg.end,
                        original=seg.text,
                        translated=seg.text  # Already Vietnamese
                    )
                    for i, seg in enumerate(segments, 1)
                ]

            if not translated_segments:
                raise Exception("No segments parsed from vietsub file")

            # Extract original audio for mixing
            audio_path = job_dir / f"audio_{job_id}.mp3"
            if not self.extract_audio(video_path, audio_path):
                raise Exception("Failed to extract original audio")
            result.audio_path = audio_path

            # Convert to TranslatedSegment format
            from app.translate import TranslatedSegment
            translated_segments = [
                TranslatedSegment(
                    index=seg.index,
                    start=seg.start,
                    end=seg.end,
                    original=seg.text,
                    translated=seg.text  # Already Vietnamese
                )
                for seg in segments
            ]

            # Save as subtitle (vietsub is both original and translated)
            subtitle_path = job_dir / f"subtitle_{job_id}.srt"
            translated_path = job_dir / f"translated_{job_id}.srt"

            # Save subtitle SRT
            srt_lines = []
            for i, seg in enumerate(translated_segments, 1):
                from app.utils import format_timestamp
                start = format_timestamp(seg.start)
                end = format_timestamp(seg.end)
                srt_lines.append(str(i))
                srt_lines.append(f"{start} --> {end}")
                srt_lines.append(seg.translated)
                srt_lines.append("")
            subtitle_path.write_text("\n".join(srt_lines), encoding="utf-8")
            translated_path.write_text("\n".join(srt_lines), encoding="utf-8")
            result.subtitle_path = subtitle_path
            result.translated_path = translated_path

            # Step 2: Generate TTS directly from vietsub (only if voiceover enabled)
            if self.config.add_voiceover:
                tts_dir = job_dir / "tts_segments"
                tts_segments = self.generate_tts(translated_segments, tts_dir)

                # Check TTS success rate
                success_count = sum(1 for s in tts_segments if not s.error)
                if success_count == 0:
                    result.warnings.append("All TTS segments failed")
                elif success_count < len(tts_segments) * 0.5:
                    result.warnings.append(f"Only {success_count}/{len(tts_segments)} TTS segments succeeded")

                # Step 3: Sync audio
                voiceover_path = job_dir / f"voiceover_{job_id}.mp3"
                if success_count > 0:
                    self.sync_audio(tts_segments, voiceover_path)
                    if voiceover_path.exists():
                        result.voiceover_path = voiceover_path
            else:
                print(f"[PIPELINE] TTS/Sync SKIPPED (voiceover disabled)")
                result.voiceover_path = None

            # Step 4: Render
            output_path = job_dir / f"final_{job_id}.mp4"
            render_result = self.render(
                video_path=video_path,
                subtitle_path=translated_path,
                voiceover_path=result.voiceover_path,
                output_path=output_path,
                original_audio_path=result.audio_path,
                voiceover_volume=self.config.voiceover_volume,
                add_subtitles=self.config.add_subtitles
            )

            if render_result.success:
                result.output_path = render_result.output_path
                result.warnings.extend(render_result.warnings)
                result.success = True
                result.step = PipelineStep.COMPLETED
            else:
                result.success = False
                result.step = PipelineStep.FAILED
                result.error = f"Render failed: {render_result.error}"
                result.warnings.append(f"Render warning: {render_result.error}")
                return result

            logger.info(f"Vietsub pipeline completed: {job_id}")

        except Exception as e:
            logger.error(f"Vietsub pipeline failed: {e}")
            result.success = False
            result.error = str(e)
            result.step = PipelineStep.FAILED

        return result

    def process(
        self,
        video_path: Path,
        job_dir: Path,
        job_id: Optional[str] = None
    ) -> PipelineResult:
        """
        Full processing pipeline.

        Args:
            video_path: Input video path
            job_dir: Working directory for job
            job_id: Job identifier

        Returns:
            PipelineResult with all output paths
        """
        job_id = job_id or generate_job_id()
        ensure_dir(job_dir)
        self.current_step = PipelineStep.INIT

        result = PipelineResult(
            success=False,
            job_id=job_id,
            video_path=video_path
        )

        try:
            # Step 1: Extract audio
            audio_path = job_dir / f"audio_{job_id}.mp3"
            if not self.extract_audio(video_path, audio_path):
                raise Exception("Failed to extract audio")
            result.audio_path = audio_path

            # Step 2: Transcribe
            segments = self.transcribe(audio_path)
            subtitle_path = job_dir / f"subtitle_{job_id}.srt"
            self.stt_service.save_srt(segments, subtitle_path)
            result.subtitle_path = subtitle_path

            # Step 3: Translate
            translated_segments = self.translate(segments)
            translated_path = job_dir / f"translated_{job_id}.srt"

            # Save translated SRT
            srt_lines = []
            for i, seg in enumerate(translated_segments, 1):
                from app.utils import format_timestamp
                start = format_timestamp(seg.start)
                end = format_timestamp(seg.end)
                srt_lines.append(str(i))
                srt_lines.append(f"{start} --> {end}")
                srt_lines.append(seg.translated)
                srt_lines.append("")

            translated_path.write_text("\n".join(srt_lines), encoding="utf-8")
            result.translated_path = translated_path

            # Step 4: Generate TTS (only if voiceover enabled)
            if self.config.add_voiceover:
                tts_dir = job_dir / "tts_segments"
                print(f"[PIPELINE] Step 4: Generate TTS")
                tts_segments = self.generate_tts(translated_segments, tts_dir)

                # Check TTS success rate
                success_count = sum(1 for s in tts_segments if not s.error)
                print(f"[PIPELINE] TTS Result: {success_count}/{len(tts_segments)}")

                # FAIL FAST: No TTS = stop
                if success_count == 0:
                    raise Exception(f"All TTS segments failed - stopping pipeline")

                # Step 5: Sync audio
                print(f"[PIPELINE] Step 5: Sync audio")
                voiceover_path = job_dir / f"voiceover_{job_id}.mp3"
                self.sync_audio(tts_segments, voiceover_path)

                # Verify voiceover was created
                if not voiceover_path.exists():
                    raise Exception(f"VOICEOVER NOT CREATED: {voiceover_path}")
                if voiceover_path.stat().st_size < 1000:
                    raise Exception(f"VOICEOVER TOO SMALL: {voiceover_path.stat().st_size} bytes")

                result.voiceover_path = voiceover_path
                print(f"[PIPELINE] Voiceover ready: {voiceover_path.stat().st_size} bytes")
            else:
                print(f"[PIPELINE] Step 4-5: SKIPPED (voiceover disabled)")
                result.voiceover_path = None

            # Step 6: Render
            print(f"[PIPELINE] Step 6: Render final video")
            output_path = job_dir / f"final_{job_id}.mp4"

            render_result = self.render(
                video_path=video_path,
                subtitle_path=translated_path,
                voiceover_path=result.voiceover_path,
                output_path=output_path,
                original_audio_path=result.audio_path,
                voiceover_volume=self.config.voiceover_volume,
                add_subtitles=self.config.add_subtitles
            )

            if render_result.success:
                result.output_path = render_result.output_path
                result.warnings.extend(render_result.warnings)
                result.success = True
                result.step = PipelineStep.COMPLETED
            else:
                result.success = False
                result.step = PipelineStep.FAILED
                result.error = f"Render failed: {render_result.error}"
                result.warnings.append(f"Render warning: {render_result.error}")
                logger.error(f"Pipeline render failed: {render_result.error}")
                return result

            logger.info(f"Pipeline completed: {job_id}")
            logger.info(f"Output: {result.output_path}")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            result.success = False
            result.error = str(e)
            result.step = PipelineStep.FAILED

        return result


def process_video(
    video_path: Path,
    job_dir: Path,
    job_id: Optional[str] = None,
    **kwargs
) -> PipelineResult:
    """
    Convenience function for full pipeline processing.

    Args:
        video_path: Input video file
        job_dir: Working directory
        job_id: Optional job ID
        **kwargs: PipelineConfig options

    Returns:
        PipelineResult
    """
    config = PipelineConfig(**kwargs) if kwargs else PipelineConfig()
    processor = PipelineProcessor(config)
    return processor.process(video_path, job_dir, job_id)
