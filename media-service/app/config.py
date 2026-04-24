from pydantic_settings import BaseSettings
from typing import Optional
from pathlib import Path
from pydantic import model_validator


class Settings(BaseSettings):
    """Application settings."""

    # Paths
    media_dir: Path = Path("./data")
    temp_dir: Optional[Path] = None
    videos_dir: Optional[Path] = None  # Chỉ lưu video đã download

    # Whisper settings
    whisper_model: str = "medium"  # medium for better accuracy (use large-v3 if GPU available)
    whisper_device: str = "cpu"
    whisper_compute_type: str = "int8"
    whisper_beam_size: int = 5
    whisper_temperature: float = 0.0
    whisper_best_of: int = 5

    # TTS settings
    tts_default_voice: str = "vi-VN-HoaiMyNeural"
    tts_rate: str = "+0%"
    tts_volume: str = "+0%"

    # Translate settings
    translate_source_lang: str = "auto"
    translate_target_lang: str = "vi"

    # Processing limits
    max_concurrent_jobs: int = 2
    job_timeout_seconds: int = 3600

    # Server
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "INFO"

    class Config:
        env_prefix = ""
        case_sensitive = False

    @model_validator(mode="after")
    def resolve_paths(self) -> "Settings":
        """Resolve dependent paths from media_dir when not explicitly set."""
        if self.temp_dir is None:
            self.temp_dir = self.media_dir / "temp"
        if self.videos_dir is None:
            self.videos_dir = self.media_dir / "videos"
        return self


# Global settings instance
settings = Settings()

# Ensure directories exist
settings.media_dir.mkdir(parents=True, exist_ok=True)
settings.temp_dir.mkdir(parents=True, exist_ok=True)
settings.videos_dir.mkdir(parents=True, exist_ok=True)  # Video đã download
