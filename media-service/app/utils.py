import os
import uuid
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List, Any
from datetime import datetime

from app.config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_job_id() -> str:
    """Generate unique job ID."""
    return f"job_{uuid.uuid4().hex[:12]}"


def generate_task_id() -> str:
    """Generate unique task ID."""
    return f"task_{uuid.uuid4().hex[:12]}"


def get_media_path(job_id: str, filename: str) -> Path:
    """Get full path for media file."""
    job_dir = settings.media_dir / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir / filename


def get_temp_path(filename: str) -> Path:
    """Get full path for temp file."""
    return settings.temp_dir / filename


def format_timestamp(seconds: float) -> str:
    """Format seconds to SRT timestamp format (HH:MM:SS,mmm)."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def parse_timestamp(timestamp: str) -> float:
    """Parse SRT timestamp to seconds."""
    try:
        parts = timestamp.replace(',', ':').split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
        millis = int(parts[3])
        return hours * 3600 + minutes * 60 + seconds + millis / 1000
    except Exception as e:
        logger.warning(f"Failed to parse timestamp {timestamp}: {e}")
        return 0.0


def ensure_dir(path: Path) -> Path:
    """Ensure directory exists."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def cleanup_temp_files(*paths: Path) -> None:
    """Clean up temporary files."""
    for path in paths:
        try:
            if path.exists():
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    import shutil
                    shutil.rmtree(path)
                logger.debug(f"Cleaned up: {path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup {path}: {e}")


def get_file_size_mb(path: Path) -> float:
    """Get file size in MB."""
    if path.exists():
        return path.stat().st_size / (1024 * 1024)
    return 0.0


def sanitize_text(text: str) -> str:
    """Sanitize text - remove only control characters, preserve all Unicode."""
    if not text:
        return text
    import re
    # Remove only control characters (ASCII 0-31 except tab/newline, and DEL)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    return text


def detect_language_code(lang: str) -> str:
    """Map language code to proper format."""
    lang_map = {
        'zh': 'zh-CN',
        'tw': 'zh-TW',
        'cn': 'zh-CN',
        'en': 'en-US',
        'vi': 'vi-VN',
        'ja': 'ja-JP',
        'ko': 'ko-KR',
        'th': 'th-TH',
        'id': 'id-ID',
        'ms': 'ms-MY',
    }
    return lang_map.get(lang.lower(), lang)


def get_tts_voice_for_language(lang: str) -> str:
    """Get appropriate TTS voice for language."""
    voice_map = {
        'vi': 'vi-VN-HoaiMyNeural',
        'en': 'en-US-AriaNeural',
        'zh': 'zh-CN-XiaoxiaoNeural',
        'ja': 'ja-JP-NanamiNeural',
        'ko': 'ko-KR-SunhiNeural',
        'th': 'th-TH-PremwadeeNeural',
        'id': 'id-ID-GadisNeural',
        'ms': 'ms-MY-YasminNeural',
        'fr': 'fr-FR-DeniseNeural',
        'de': 'de-DE-KatjaNeural',
        'es': 'es-ES-ElviraNeural',
        'pt': 'pt-BR-FranciscaNeural',
        'ru': 'ru-RU-DariyaNeural',
        'ar': 'ar-SA-ZariyahNeural',
    }
    lang_base = lang.split('-')[0].lower()
    return voice_map.get(lang_base, 'vi-VN-HoaiMyNeural')


@dataclass
class SubtitleSegment:
    """Subtitle segment data structure."""
    index: int
    start: float
    end: float
    text: str


def parse_srt_file(srt_path: Path) -> List[SubtitleSegment]:
    """Parse SRT file and return list of SubtitleSegment."""
    if not srt_path.exists():
        raise FileNotFoundError(f"SRT file not found: {srt_path}")
    
    content = srt_path.read_text(encoding="utf-8")
    segments = []
    lines_list = content.strip().split("\n")
    
    i = 0
    segment_idx = 0
    while i < len(lines_list):
        line = lines_list[i].strip()
        
        # Check if this line is a timestamp
        if " --> " in line:
            try:
                start_str, end_str = line.split(" --> ")
                start = parse_timestamp(start_str)
                end = parse_timestamp(end_str)
                
                # Next line(s) are text
                i += 1
                text_lines = []
                while i < len(lines_list) and lines_list[i].strip() and " --> " not in lines_list[i]:
                    text_lines.append(lines_list[i].strip())
                    i += 1
                
                text = " ".join(text_lines)
                segment_idx += 1
                
                segments.append(SubtitleSegment(
                    index=segment_idx,
                    start=start,
                    end=end,
                    text=text
                ))
                continue
            except:
                pass
        
        i += 1
    
    return segments


def parse_vietsub_dual_format(srt_path: Path) -> List[Tuple[SubtitleSegment, str]]:
    """
    Parse vietsub file in format:
        00:00:00,000 --> 00:00:08,000
        原文 (Trung)
        译文 (Viet)
    
    Returns list of (segment, viet_translation) tuples.
    """
    if not srt_path.exists():
        raise FileNotFoundError(f"SRT file not found: {srt_path}")
    
    content = srt_path.read_text(encoding="utf-8")
    lines = content.strip().split("\n")
    
    result = []
    i = 0
    idx = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        if " --> " in line:
            try:
                idx += 1
                start_str, end_str = line.split(" --> ")
                start = parse_timestamp(start_str)
                end = parse_timestamp(end_str)
                
                i += 1
                original = ""
                while i < len(lines) and not lines[i].strip():
                    i += 1
                if i < len(lines):
                    original = lines[i].strip()
                
                i += 1
                viet = ""
                while i < len(lines) and not lines[i].strip():
                    i += 1
                if i < len(lines):
                    viet = lines[i].strip()
                
                result.append((
                    SubtitleSegment(index=idx, start=start, end=end, text=original),
                    viet
                ))
                i += 1
                continue
            except Exception as e:
                logger.warning(f"Parse error at line {i}: {e}")
        
        i += 1
    
    return result


class TimingInfo:
    """Container for timing information."""

    def __init__(self, start: float, end: float, text: str = ""):
        self.start = start
        self.end = end
        self.text = text
        self.duration = end - start

    def __repr__(self):
        return f"TimingInfo(start={self.start:.2f}, end={self.end:.2f}, text='{self.text[:30]}...')"


def create_ass_subtitle_file(
    segments: List[Tuple[float, float, str]],
    output_path: Path,
    style: str = "professional"
) -> Path:
    """
    Create professional ASS subtitle file from segments.

    Args:
        segments: List of (start, end, text) tuples
        output_path: Output .ass file path
        style: Style preset - "professional" (Netflix/YouTube), "clean", "minimal"

    Returns:
        Path to created ASS file
    """
    if style == "professional":
        # Netflix/YouTube style - compact, readable
        font_size = 28
        primary_color = "&H00FFFFFF"  # White
        outline_color = "&H00000000"  # Black outline
        back_color = "&H80000000"     # Semi-transparent black background
        outline = 1.5
        shadow = 2.0
        margin_l = 15
        margin_r = 15
        margin_v = 15
        alignment = 5  # Bottom center
        font_name = "Arial"
    elif style == "clean":
        # Clean style - no background, just shadow
        font_size = 26
        primary_color = "&H00FFFFFF"
        outline_color = "&H00000000"
        back_color = "&H00000000"
        outline = 1.0
        shadow = 1.5
        margin_l = 10
        margin_r = 10
        margin_v = 10
        alignment = 5
        font_name = "Inter"
    else:
        # Minimal - smaller, subtle
        font_size = 22
        primary_color = "&H00FFFFFF"
        outline_color = "&H00000000"
        back_color = "&H00000000"
        outline = 0.8
        shadow = 1.0
        margin_l = 10
        margin_r = 10
        margin_v = 10
        alignment = 5
        font_name = "Arial"

    ass_header = f"""[Script Info]
ScriptType: v4.00+
WrapStyle: 0
ScaledBorderAndShadow: yes
YCbCr Matrix: None

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,{font_name},{font_size},{primary_color},{primary_color},{outline_color},{back_color},0,0,0,0,100,100,0,0,1,{outline},{shadow},{alignment},{margin_l},{margin_r},{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    ass_events = []
    for start, end, text in segments:
        # Convert seconds to ASS timestamp (H:MM:SS.CC)
        def format_ass_time(seconds):
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = seconds % 60
            centisecs = int((secs % 1) * 100)
            whole_secs = int(secs)
            return f"{hours}:{minutes:02d}:{whole_secs:02d}.{centisecs:02d}"

        start_time = format_ass_time(start)
        end_time = format_ass_time(end)

        # Escape text for ASS format
        escaped_text = text.replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")
        ass_events.append(f"Dialogue: 0,{start_time},{end_time},Default,,0,0,0,,{escaped_text}")

    ass_content = ass_header + "\n".join(ass_events)

    output_path.write_text(ass_content, encoding="utf-8-sig")
    logger.info(f"Created ASS subtitle file: {output_path} ({len(segments)} segments, style={style})")

    return output_path
