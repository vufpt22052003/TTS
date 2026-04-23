"""
Video Rendering Module using ffmpeg.

Pipeline chuẩn:
1. Extract audio từ video
2. Generate TTS với Edge TTS
3. Sync audio với timing mềm (preserve pauses)
4. Mix voiceover + original audio
5. Burn subtitles

Error handling: FAIL FAST - không render nếu thiếu audio
"""

import subprocess
import logging
import os
import json
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RenderResult:
    """Result of render operation."""
    success: bool
    output_path: Optional[Path]
    error: Optional[str]
    warnings: List[str]


class RenderService:
    """Video rendering service using ffmpeg."""

    def __init__(self):
        self.ffmpeg_path = self._get_ffmpeg_path()
        self._check_ffmpeg()

    def _get_ffmpeg_path(self) -> str:
        """Get ffmpeg path."""
        import shutil
        if shutil.which('ffmpeg'):
            return 'ffmpeg'

        common_paths = [
            r"C:\ffmpeg\bin\ffmpeg.exe",
            r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
            r"D:\ffmpeg\bin\ffmpeg.exe",
        ]
        for path in common_paths:
            if Path(path).exists():
                return path

        return 'ffmpeg'

    def _check_ffmpeg(self) -> bool:
        """Check if ffmpeg is available."""
        try:
            result = subprocess.run(
                [self.ffmpeg_path, '-version'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                logger.info("ffmpeg is available")
                return True
        except FileNotFoundError:
            logger.error("ffmpeg not found")
            return False
        return False

    def _run_ffmpeg(self, args: List[str], timeout: int = 3600) -> Tuple[bool, str, str]:
        """Run ffmpeg command."""
        try:
            result = subprocess.run(
                [self.ffmpeg_path, '-y'] + args,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            logger.error("ffmpeg timeout")
            return False, "", "Timeout"
        except Exception as e:
            logger.error(f"ffmpeg error: {e}")
            return False, "", str(e)

    def extract_audio(self, video_path: Path, output_path: Path) -> bool:
        """Extract audio from video."""
        if not video_path.exists():
            logger.error(f"Video not found: {video_path}")
            return False

        cmd = [
            '-i', str(video_path),
            '-vn',
            '-acodec', 'libmp3lame',
            '-ab', '192k',
            '-ar', '44100',
            '-ac', '2',
            str(output_path)
        ]

        success, _, stderr = self._run_ffmpeg(cmd)
        if success:
            logger.info(f"Audio extracted: {output_path}")
        else:
            logger.error(f"Audio extraction failed: {stderr}")
        return success

    def combine_audio_segments(
        self,
        segments: List[Dict],
        output_path: Path,
        base_duration: float = 0.0
    ) -> bool:
        """
        Combine audio segments with PROPER timing and pauses.
        FAIL FAST if no valid segments or file not created.
        """
        valid_segments = [
            seg for seg in segments
            if seg.get('audio_path') and Path(seg['audio_path']).exists()
        ]

        print(f"[COMBINE] Valid segments: {len(valid_segments)}/{len(segments)}")

        # FAIL FAST: No audio segments
        if not valid_segments:
            raise Exception("NO AUDIO SEGMENTS TO COMBINE")

        # Analyze timing
        for seg in valid_segments:
            orig_dur = seg.get('end', 0) - seg.get('start', 0)
            tts_dur = seg.get('duration', 0)
            ratio = tts_dur / orig_dur if orig_dur > 0 else 1.0
            gap = seg.get('gap', 0)

            print(f"[COMBINE] Seg {seg.get('start', 0):.1f}s: "
                  f"TTS={tts_dur:.2f}s, Orig={orig_dur:.2f}s, Ratio={ratio:.2f}")

        # Single segment
        if len(valid_segments) == 1:
            seg = valid_segments[0]
            orig_dur = seg['end'] - seg['start']
            tts_dur = seg.get('duration', 1.0)
            ratio = tts_dur / orig_dur if orig_dur > 0 else 1.0

            if 0.7 <= ratio <= 1.3:
                if tts_dur < orig_dur:
                    output = self._pad_audio(seg['audio_path'], output_path, orig_dur)
                else:
                    output = self._trim_audio(seg['audio_path'], output_path, orig_dur)
            else:
                output = self._pad_audio(seg['audio_path'], output_path, orig_dur)

            if output:
                # CHECK: File created?
                if output_path.exists() and output_path.stat().st_size > 1000:
                    print(f"[COMBINE] Single segment synced: {output_path}")
                    return True
                else:
                    raise Exception(f"VOICEOVER FILE NOT CREATED: {output_path}")
            return False

        # Multiple segments: use FFmpeg adelay to place each TTS at exact start time,
        # then mix all into one audio. No trimming/cutting needed - TTS plays naturally.
        prev_end = 0.0
        delayed_files = []

        for i, seg in enumerate(valid_segments):
            seg_start = seg.get('start', 0)
            seg_end = seg.get('end', 0)
            orig_dur = seg_end - seg_start
            tts_dur = seg.get('duration', 1.0)
            tts_path = seg['audio_path']

            # Check if TTS file exists
            if not Path(tts_path).exists():
                logger.error(f"[COMBINE] TTS file not found: {tts_path}")
                raise Exception(f"TTS file not found for segment {i}: {tts_path}")

            # Calculate delay in milliseconds to place this TTS at correct start time
            delay_ms = int(seg_start * 1000)

            # Debug: log segment info
            file_size = Path(tts_path).stat().st_size if Path(tts_path).exists() else 0
            print(f"[COMBINE] Segment {i}: start={seg_start:.2f}s, delay={delay_ms}ms, file={tts_path}, size={file_size} bytes")

            # Skip empty TTS files - create silent placeholder
            if file_size == 0:
                logger.warning(f"[COMBINE] Empty TTS file, creating silence: {tts_path}")
                temp_delayed = output_path.parent / f"temp_delayed_{i:03d}.mp3"
                silent_dur = max(tts_dur, 0.1)
                silent_cmd = [
                    '-f', 'lavfi',
                    '-i', f'anullsrc=r=44100:cl=stereo',
                    '-t', str(silent_dur),
                    '-acodec', 'libmp3lame',
                    '-ab', '192k',
                    str(temp_delayed).replace('\\', '/')
                ]
                success, _, _ = self._run_ffmpeg(silent_cmd)
                if success:
                    delayed_files.append(temp_delayed)
                    continue
                else:
                    logger.error(f"[COMBINE] Failed to create silence for segment {i}")
                    continue

            temp_delayed = output_path.parent / f"temp_delayed_{i:03d}.mp3"
            delayed_files.append(temp_delayed)

            # Use adelay to offset TTS to exact start position (in ms)
            # Also boost volume to match video audio better
            # Convert backslashes to forward slashes for FFmpeg
            tts_path_str = str(tts_path).replace('\\', '/')
            temp_delayed_str = str(temp_delayed).replace('\\', '/')
            
            cmd = [
                '-i', tts_path_str,
                '-af', f'volume=1.5,adelay={delay_ms}|{delay_ms}',
                '-acodec', 'libmp3lame',
                '-ab', '192k',
                temp_delayed_str  # _run_ffmpeg already adds -y
            ]
            success, stdout, stderr = self._run_ffmpeg(cmd)
            if not success:
                # Get full error, skipping version header
                error_lines = stderr.split('\n')
                actual_error_parts = []
                for j, line in enumerate(error_lines):
                    if 'Error' in line or 'error' in line or 'Invalid' in line or 'failed' in line:
                        # Get this line and next 3 lines
                        actual_error_parts = error_lines[j:j+5]
                        break
                
                if actual_error_parts:
                    actual_error = '\n'.join(actual_error_parts)
                else:
                    # Just get last 300 chars after version header
                    after_header = stderr
                    for marker in ['built with', 'configuration:']:
                        if marker in after_header:
                            parts = after_header.split(marker, 1)
                            if len(parts) > 1:
                                after_header = parts[1]
                    actual_error = after_header.strip()[-300:]
                
                logger.error(f"[COMBINE] adelay failed for segment {i}: {actual_error}")
                logger.error(f"[COMBINE] Full stderr length: {len(stderr)}")
                raise Exception(f"Failed to delay segment {i}: {actual_error}")

        # Mix all delayed TTS tracks into one audio
        # amix normalize=0 to prevent volume reduction, then boost after
        mix_cmd = ['-i', str(delayed_files[0])]
        for f in delayed_files[1:]:
            mix_cmd.extend(['-i', str(f)])
        mix_cmd.extend([
            '-filter_complex', f'amix=inputs={len(delayed_files)}:duration=longest:dropout_transition=0:normalize=0[out];[out]volume={min(len(delayed_files), 20) * 1.5}[aout]',
            '-map', '[aout]',
            '-acodec', 'libmp3lame',
            '-ab', '192k',
            '-y',
            str(output_path)
        ])
        success, _, stderr = self._run_ffmpeg(mix_cmd)

        # Cleanup delayed files
        for f in delayed_files:
            if f.exists():
                try:
                    f.unlink()
                except:
                    pass

        if success and output_path.exists():
            print(f"[COMBINE] Multi-segment synced via adelay: {output_path}")
            return True
        logger.error(f"[COMBINE] amix failed: {stderr[:300]}")
        return False

        # Verify output file
        if not output_path.exists() or output_path.stat().st_size < 1000:
            raise Exception(f"SYNC OUTPUT FILE NOT CREATED: {output_path}")
        print(f"[COMBINE] Multi-segment synced: {output_path}")
        return True

    def _create_silence(self, output_path: Path, duration: float) -> bool:
        """Create silence audio file."""
        cmd = [
            '-f', 'lavfi',
            '-i', f'anullsrc=r=44100:cl=stereo',
            '-t', f'{duration:.3f}',
            '-acodec', 'libmp3lame',
            '-y',
            str(output_path)
        ]
        success, _, _ = self._run_ffmpeg(cmd)
        return success

    def _pad_audio(self, input_path: Path, output_path: Path, target_duration: float) -> bool:
        """Pad audio with silence to reach target duration."""
        cmd = [
            '-i', str(input_path),
            '-af', f'apad=whole_dur={target_duration:.3f}',
            '-acodec', 'libmp3lame',
            '-ab', '192k',
            '-t', f'{target_duration:.3f}',
            '-y',
            str(output_path)
        ]
        success, _, _ = self._run_ffmpeg(cmd)
        return success

    def _trim_audio(self, input_path: Path, output_path: Path, target_duration: float) -> bool:
        """Trim audio to target duration."""
        cmd = [
            '-i', str(input_path),
            '-t', f'{target_duration:.3f}',
            '-acodec', 'libmp3lame',
            '-ab', '192k',
            '-y',
            str(output_path)
        ]
        success, _, _ = self._run_ffmpeg(cmd)
        return success

    def _adjust_speed(self, input_path: Path, output_path: Path, speed_factor: float, target_duration: float) -> bool:
        """Adjust audio speed using atempo."""
        atempo = max(0.5, min(speed_factor, 2.0))

        cmd = [
            '-i', str(input_path),
            '-af', f'atempo={atempo:.3f}',
            '-t', f'{target_duration:.3f}',
            '-acodec', 'libmp3lame',
            '-ab', '192k',
            '-y',
            str(output_path)
        ]
        success, _, _ = self._run_ffmpeg(cmd)
        return success

    def _concat_segments(self, temp_files: List[Path], output_path: Path) -> bool:
        """Concatenate multiple audio segments (re-encode to ensure compatibility)."""
        valid_files = [f for f in temp_files if f.exists()]
        if not valid_files:
            logger.error(f"[CONCAT] No valid files to concat. temp_files={temp_files}")
            return False

        list_file = output_path.parent / "concat_list.txt"
        try:
            list_content = "\n".join([f"file '{f.absolute()}'" for f in valid_files])
            list_file.write_text(list_content)
        except Exception as e:
            logger.error(f"[CONCAT] Failed to write concat list: {e}")
            return False

        logger.info(f"[CONCAT] Concat list ({len(valid_files)} files): {list_file}")
        logger.info(f"[CONCAT] Content:\n{list_content}")

        # Re-encode to ensure all segments have same format/codec
        cmd = [
            '-f', 'concat',
            '-safe', '0',
            '-i', str(list_file),
            '-acodec', 'libmp3lame',
            '-ab', '192k',
            '-y',
            str(output_path)
        ]

        success, stdout, stderr = self._run_ffmpeg(cmd)

        # Cleanup
        for f in temp_files:
            if f.exists():
                try:
                    f.unlink()
                except:
                    pass
        if list_file.exists():
            try:
                list_file.unlink()
            except:
                pass

        if success and output_path.exists():
            print(f"[CONCAT] Success: {output_path}")
            return True
        logger.error(f"[CONCAT] FFmpeg failed. returncode handling unclear, stdout={stdout[:200]}, stderr={stderr[:500]}")
        print(f"[CONCAT] Failed: {stderr[:500]}")
        return False

    def create_subtitle_file(
        self,
        segments: List,
        output_path: Path,
        use_translated: bool = True
    ) -> bool:
        """Create ASS subtitle file."""
        if not segments:
            logger.warning("No segments for subtitle")
            return False

        ass_content = """[Script Info]
Title: Vietnamese Subtitles
ScriptType: v4.00+
WrapStyle: 0
ScaledBorderAndShadow: yes
YCbCr Matrix: None

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial,28,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,2,2,2,10,10,10,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

        for seg in segments:
            text = seg.translated if (use_translated and hasattr(seg, 'translated')) else seg.text
            if hasattr(seg, 'text') and not hasattr(seg, 'translated'):
                text = seg.text

            start = self._format_ass_timestamp(seg.start)
            end = self._format_ass_timestamp(seg.end)

            text = text.replace('\\', '\\\\').replace('{', '\\{').replace('}', '\\}')
            text = text.replace('\n', '\\N')

            ass_content += f"Dialogue: 0,{start},{end},Default,,0,0,0,,{text}\n"

        output_path.write_text(ass_content, encoding='utf-8')
        logger.info(f"Subtitle file created: {output_path}")
        return True

    def _format_ass_timestamp(self, seconds: float) -> str:
        """Format timestamp for ASS format."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        centisecs = int((seconds % 1) * 100)
        return f"{hours}:{minutes:02d}:{secs:02d}.{centisecs:02d}"

    def render_video(
        self,
        video_path: Path,
        subtitle_path: Path,
        audio_path: Optional[Path],
        output_path: Path,
        voiceover_path: Optional[Path] = None,
        original_audio_path: Optional[Path] = None,
        subtitle_track: bool = True,
        voiceover_volume: float = 0.8
    ) -> RenderResult:
        """
        Render final video with subtitles and voiceover.
        FAIL FAST if voiceover missing.
        """
        warnings = []
        temp_outputs = []

        # Note: voiceover is optional - if not provided, original audio is kept
        if voiceover_path and voiceover_path.exists():
            print(f"[RENDER] VOICEOVER exists: {voiceover_path.exists()}, size: {voiceover_path.stat().st_size} bytes")
        else:
            print(f"[RENDER] No voiceover - will keep original audio only")

        # Step 1: Add subtitles to video
        temp_video = output_path.parent / "temp_with_sub.mp4"
        temp_outputs.append(temp_video)

        print(f"[RENDER] subtitle_track={subtitle_track}, subtitle_path={subtitle_path}, exists={subtitle_path.exists() if subtitle_path else 'N/A'}")

        if subtitle_track and subtitle_path.exists():
            escaped_path = str(subtitle_path).replace('\\', '/').replace(':', '\\\\:')

            cmd = [
                '-i', str(video_path),
                '-vf', f"subtitles={escaped_path}:force_style='FontSize=28'",
                '-c:a', 'copy',
                str(temp_video)
            ]

            success, _, stderr = self._run_ffmpeg(cmd)

            if not success:
                ass_path = f"file={subtitle_path.as_posix()}"
                cmd = [
                    '-i', str(video_path),
                    '-vf', f"subtitles={ass_path}",
                    '-c:a', 'copy',
                    str(temp_video)
                ]
                success, _, stderr = self._run_ffmpeg(cmd)

            if not success:
                cmd = [
                    '-i', str(video_path),
                    '-vf', f"subtitles='{subtitle_path.as_posix()}'",
                    '-c:a', 'copy',
                    str(temp_video)
                ]
                success, _, stderr = self._run_ffmpeg(cmd)

            if not success:
                warnings.append(f"Subtitle burn-in failed")
                logger.warning(f"Could not add subtitles: {stderr[:200]}")
                import shutil
                shutil.copy(video_path, temp_video)
        else:
            temp_video = video_path

        # Step 2: Mix audio
        print(f"[RENDER] voiceover_path={voiceover_path}, exists={voiceover_path.exists() if voiceover_path else 'N/A'}")
        print(f"[RENDER] original_audio_path={original_audio_path}, exists={original_audio_path.exists() if original_audio_path else 'N/A'}")
        if voiceover_path and voiceover_path.exists():
            base_audio = original_audio_path if original_audio_path and original_audio_path.exists() else None

            if base_audio:
                temp_mixed = output_path.parent / "temp_mixed_audio.mp3"
                temp_outputs.append(temp_mixed)

                cmd = [
                    '-i', str(temp_video),
                    '-i', str(base_audio),
                    '-i', str(voiceover_path),
                    '-filter_complex',
                    f'[1:a]volume=0.05[orig];[2:a]volume=1.2[voiced];[orig][voiced]amix=inputs=2:duration=longest[aout]',
                    '-map', '0:v',
                    '-map', '[aout]',
                    '-c:v', 'copy',
                    '-c:a', 'aac',
                    '-b:a', '192k',
                    str(output_path)
                ]
            else:
                cmd = [
                    '-i', str(temp_video),
                    '-i', str(voiceover_path),
                    '-map', '0:v',
                    '-map', '1:a',
                    '-c:v', 'copy',
                    '-c:a', 'aac',
                    '-b:a', '192k',
                    str(output_path)
                ]

            success, _, stderr = self._run_ffmpeg(cmd)

            if not success:
                warnings.append(f"Audio mixing failed")
                logger.warning(f"Could not mix audio: {stderr}")
                import shutil
                shutil.copy(temp_video, output_path)
        else:
            # No voiceover - copy video with subtitles (if any) as final output
            import shutil
            if temp_video != video_path:
                shutil.copy(temp_video, output_path)
            else:
                shutil.copy(video_path, output_path)

        # Cleanup
        for temp in temp_outputs:
            try:
                if temp.exists() and temp != output_path:
                    temp.unlink()
            except Exception as e:
                logger.warning(f"Cleanup error: {e}")

        return RenderResult(
            success=output_path.exists(),
            output_path=output_path,
            error=None if output_path.exists() else "Render failed",
            warnings=warnings
        )

    def render_full(
        self,
        video_path: Path,
        subtitle_path: Path,
        voiceover_path: Optional[Path],
        original_audio_path: Optional[Path],
        output_path: Path,
        voiceover_volume: float = 0.8,
        subtitle_track: bool = True
    ) -> RenderResult:
        """Full render pipeline."""
        return self.render_video(
            video_path=video_path,
            subtitle_path=subtitle_path,
            audio_path=None,
            output_path=output_path,
            voiceover_path=voiceover_path,
            original_audio_path=original_audio_path,
            subtitle_track=subtitle_track,
            voiceover_volume=voiceover_volume
        )


def render_final_video(
    video_path: Path,
    subtitle_path: Path,
    voiceover_path: Optional[Path],
    original_audio_path: Optional[Path],
    output_path: Path
) -> RenderResult:
    """Convenience function for rendering."""
    service = RenderService()
    return service.render_full(
        video_path=video_path,
        subtitle_path=subtitle_path,
        voiceover_path=voiceover_path,
        original_audio_path=original_audio_path,
        output_path=output_path
    )
