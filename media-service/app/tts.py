"""
Text-to-Speech Module using gTTS (Google Translate).

Primary: gTTS (reliable, free)
"""

import logging
import os
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class TTSSegment:
    """TTS segment with audio info."""
    index: int
    start: float
    end: float
    text: str
    audio_path: Optional[Path] = None
    duration: float = 0.0
    error: Optional[str] = None


VOICE_MAP = {
    'vi': 'vi',
    'en': 'en',
    'zh': 'zh-CN',
    'ja': 'ja',
    'ko': 'ko',
    'fr': 'fr',
    'de': 'de',
    'es': 'es',
    'pt': 'pt',
    'ru': 'ru',
    'ar': 'ar',
    'th': 'th',
    'id': 'id',
    'ms': 'ms',
}


def extract_gtts_lang(voice: str) -> str:
    """Extract gTTS language code from voice name like 'vi-VN-HoaiMyNeural' -> 'vi'"""
    if not voice:
        return 'vi'
    
    # Handle Azure voice names like 'vi-VN-HoaiMyNeural'
    if '-' in voice:
        lang_part = voice.split('-')[0].lower()
        if lang_part in VOICE_MAP:
            return VOICE_MAP[lang_part]
    
    # Handle direct codes like 'vi', 'en', 'zh-CN'
    return VOICE_MAP.get(voice.lower(), voice)


class TTSService:
    """Text-to-Speech service using gTTS."""

    def __init__(self, voice: Optional[str] = None):
        self.voice = voice

    def _get_voice_for_language(self, language: str = 'vi') -> str:
        """Get appropriate voice for language."""
        if self.voice:
            return extract_gtts_lang(self.voice)
        return VOICE_MAP.get(language, 'vi')

    def _get_audio_duration(self, audio_path: Path) -> float:
        """Get audio duration using ffprobe."""
        try:
            import subprocess
            cmd = [
                'C:\\ffmpeg\\bin\\ffprobe.exe',
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                str(audio_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return float(result.stdout.strip())
        except Exception as e:
            logger.warning(f"Failed to get audio duration: {e}")
            return 0.0

    def generate_segments(
        self,
        segments: List,
        output_dir: Path,
        voice: Optional[str] = None,
        language: str = 'vi'
    ) -> List[TTSSegment]:
        """Generate TTS for all segments."""
        output_dir.mkdir(parents=True, exist_ok=True)
        target_lang = voice or self._get_voice_for_language(language)
        # Extract gTTS lang code from voice name
        gtts_lang = extract_gtts_lang(target_lang)

        print(f"[TTS] Generating {len(segments)} segments with lang: {gtts_lang}")
        logger.info(f"Generating TTS for {len(segments)} segments with lang: {target_lang}")

        tts_segments = []

        for seg in segments:
            text = seg.translated if hasattr(seg, 'translated') else (seg.text if hasattr(seg, 'text') else str(seg))
            output_path = output_dir / f"tts_{seg.index:04d}.mp3"

            tts_seg = TTSSegment(
                index=seg.index,
                start=seg.start,
                end=seg.end,
                text=text,
                audio_path=output_path
            )

            try:
                success, duration = self._generate_gtts(text, output_path, target_lang)
                if success and duration > 0.2:
                    tts_seg.duration = duration
                    print(f"[TTS] OK [{seg.index}]: {output_path} ({duration:.2f}s)")
                else:
                    tts_seg.error = "invalid audio"
                    print(f"[TTS] INVALID [{seg.index}]: {duration:.2f}s")
            except Exception as e:
                tts_seg.error = str(e)
                print(f"[TTS] FAIL [{seg.index}]: {e}")
                logger.error(f"TTS FAIL [{seg.index}]: {e}")

            tts_segments.append(tts_seg)

        success_count = sum(1 for s in tts_segments if not s.error)
        print(f"[TTS] Results: {success_count}/{len(tts_segments)} successful")
        logger.info(f"TTS completed: {success_count}/{len(tts_segments)} successful")

        if success_count == 0:
            raise Exception("TTS FAILED COMPLETELY - no valid audio generated")

        return tts_segments

    def _generate_gtts(self, text: str, output_path: Path, lang: str) -> Tuple[bool, float]:
        """Generate TTS using gTTS."""
        from gtts import gTTS
        
        # Extract gTTS lang code
        gtts_lang = extract_gtts_lang(lang)
        tts = gTTS(text=text, lang=gtts_lang, slow=False)
        tts.save(str(output_path))
        
        duration = self._get_audio_duration(output_path)
        return True, duration

    def generate_full_audio(
        self,
        text: str,
        output_path: Path,
        voice: Optional[str] = None,
        language: str = 'vi'
    ) -> Tuple[bool, Optional[str]]:
        """Generate TTS for full text."""
        target_lang = voice or self._get_voice_for_language(language)
        gtts_lang = extract_gtts_lang(target_lang)

        try:
            from gtts import gTTS
            tts = gTTS(text=text, lang=gtts_lang, slow=False)
            tts.save(str(output_path))
            return True, None
        except Exception as e:
            logger.error(f"TTS failed: {e}")
            return False, str(e)


def generate_tts_segments(
    segments: List,
    output_dir: Path,
    voice: Optional[str] = None,
    language: str = 'vi'
) -> List[TTSSegment]:
    """Convenience function for TTS generation."""
    service = TTSService(voice=voice)
    return service.generate_segments(segments, output_dir, voice, language)
