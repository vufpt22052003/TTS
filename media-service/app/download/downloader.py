"""
Video Downloader - Multi-Strategy Download Engine

Supports multiple download strategies:
1. yt-dlp with cookies
2. yt-dlp with browser cookies
3. Direct URL extraction and download
"""

import os
import re
import time
import logging
import subprocess
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)


class DownloadStrategy(Enum):
    """Available download strategies."""
    YT_DLP_COOKIES = "yt_dlp_cookies"
    YT_DLP_BROWSER = "yt_dlp_browser"
    YT_DLP_DIRECT = "yt_dlp_direct"
    DIRECT_URL = "direct_url"
    NONE = "none"


class Platform(Enum):
    """Supported video platforms."""
    YOUTUBE = "youtube"
    TIKTOK = "tiktok"
    TWITTER = "twitter"
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"
    UNKNOWN = "unknown"
    
    @classmethod
    def detect(cls, url: str) -> "Platform":
        """Detect platform from URL."""
        parsed = urlparse(url.lower())
        domain = parsed.netloc.replace("www.", "").replace("m.", "")
        
        platform_map = {
            "youtube.com": cls.YOUTUBE,
            "youtu.be": cls.YOUTUBE,
            "tiktok.com": cls.TIKTOK,
            "vm.tiktok.com": cls.TIKTOK,
            "twitter.com": cls.TWITTER,
            "x.com": cls.TWITTER,
            "fb.com": cls.FACEBOOK,
            "facebook.com": cls.FACEBOOK,
            "instagram.com": cls.INSTAGRAM,
        }
        
        return platform_map.get(domain, cls.UNKNOWN)


@dataclass
class DownloadResult:
    """Result of a download attempt."""
    success: bool
    strategy_used: Optional[DownloadStrategy]
    file_path: Optional[Path]
    file_size: int
    error: Optional[str]
    duration_seconds: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def human_readable_size(self) -> str:
        """Convert file size to human readable format."""
        size = self.file_size
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} TB"


class VideoDownloader:
    """
    Multi-Strategy Video Downloader.
    
    Tries download strategies in order until one succeeds.
    """
    
    # User agents for rotation
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    ]
    
    # Platform-specific settings
    PLATFORM_SETTINGS: Dict[Platform, Dict[str, Any]] = {
        Platform.TIKTOK: {
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "http_headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://www.tiktok.com/",
            },
        },
        Platform.YOUTUBE: {
            "format": "bestvideo[ext=mp4]/best[ext=mp4]/best",
            "http_headers": {
                "User-Agent": USER_AGENTS[0],
            },
        },
        Platform.TWITTER: {
            "format": "best[ext=mp4]/best",
            "http_headers": {
                "User-Agent": USER_AGENTS[0],
            },
        },
        Platform.UNKNOWN: {
            "format": "bestvideo+bestaudio/best",
        }
    }
    
    def __init__(
        self,
        output_dir: Path,
        venv_path: Optional[Path] = None,
        cookies_file: Optional[Path] = None,
        max_retries: int = 3,
        timeout: int = 600,
    ):
        """
        Initialize downloader.
        
        Args:
            output_dir: Directory to save downloaded videos
            venv_path: Path to virtual environment (for yt-dlp)
            cookies_file: Path to cookies.txt file
            max_retries: Maximum retry attempts per strategy
            timeout: Download timeout in seconds
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.venv_path = Path(venv_path) if venv_path else None
        self.cookies_file = cookies_file
        self.max_retries = max_retries
        self.timeout = timeout
        
        # Find yt-dlp
        self.yt_dlp_path = self._find_yt_dlp()
        
        # Detect platform from URL
        self.platform: Optional[Platform] = None
        self.current_url: Optional[str] = None
    
    def _find_yt_dlp(self) -> Optional[Path]:
        """Find yt-dlp executable or Python module."""
        # Check venv
        if self.venv_path:
            yt_dlp_exe = self.venv_path / "Scripts" / "yt-dlp.exe"
            if yt_dlp_exe.exists():
                return yt_dlp_exe
            
            yt_dlp_py = self.venv_path / "Scripts" / "yt-dlp.exe"
            if yt_dlp_py.exists():
                return yt_dlp_py
        
        # Check if yt-dlp is available via Python
        try:
            import yt_dlp
            logger.info("yt-dlp available as Python module")
            return None  # Will use Python module
        except ImportError:
            pass
        
        # Try system PATH
        for name in ["yt-dlp", "yt-dlp.exe"]:
            result = subprocess.run(
                ["where" if os.name == "nt" else "which", name],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                path = Path(result.stdout.strip().split('\n')[0])
                logger.info(f"Found yt-dlp at: {path}")
                return path
        
        logger.warning("yt-dlp not found - using fallback direct URL method")
        return None
    
    def _get_platform_settings(self) -> Dict[str, Any]:
        """Get platform-specific settings."""
        if not self.platform:
            return self.PLATFORM_SETTINGS[Platform.UNKNOWN]
        return self.PLATFORM_SETTINGS.get(self.platform, self.PLATFORM_SETTINGS[Platform.UNKNOWN])

    def _normalize_extracted_url(self, url: str) -> str:
        """Normalize escaped URLs extracted from HTML/JSON blobs."""
        normalized = url.strip()
        normalized = normalized.replace("\\u002F", "/").replace("\\/", "/")
        normalized = normalized.replace("\\u0026", "&").replace("\\u003D", "=")
        normalized = normalized.replace("\\\\", "")
        return normalized
    
    def _get_ytdlp_opts(self, output_path: Path, use_cookies: bool = False) -> Dict[str, Any]:
        """Build yt-dlp options."""
        platform_settings = self._get_platform_settings()
        
        opts = {
            "format": platform_settings.get("format", "best"),
            "outtmpl": str(output_path),
            "noplaylist": True,
            "quiet": False,
            "no_warnings": False,
            "extract_flat": False,
            # Force .mp4 extension - critical for TikTok and other platforms
            "ext": "mp4",
        }
        
        # Add platform-specific headers
        if "http_headers" in platform_settings:
            opts["http_headers"] = platform_settings["http_headers"]
        else:
            opts["http_headers"] = {
                "User-Agent": self.USER_AGENTS[0]
            }
        
        # Add cookies if available
        if use_cookies and self.cookies_file and self.cookies_file.exists():
            opts["cookiefile"] = str(self.cookies_file)
        
        return opts
    
    def download(self, url: str, job_id: str) -> DownloadResult:
        """
        Download video using multi-strategy approach.
        
        Args:
            url: Video URL
            job_id: Job ID for filename
            
        Returns:
            DownloadResult with download status and details
        """
        start_time = time.time()
        self.current_url = url
        self.platform = Platform.detect(url)
        
        logger.info(f"Starting download: {url}")
        logger.info(f"Detected platform: {self.platform.value}")
        
        # Define strategies to try in order
        strategies = [
            DownloadStrategy.YT_DLP_COOKIES,
            DownloadStrategy.YT_DLP_BROWSER,
            DownloadStrategy.YT_DLP_DIRECT,
            DownloadStrategy.DIRECT_URL,
        ]
        
        for strategy in strategies:
            if strategy == DownloadStrategy.NONE:
                continue
                
            result = self._try_strategy(strategy, url, job_id)
            
            if result.success:
                logger.info(
                    f"✓ Download succeeded using {strategy.value} "
                    f"({result.human_readable_size})"
                )
                return result
            
            logger.warning(f"✗ Strategy {strategy.value} failed: {result.error}")
            
            # Clean up failed download
            if result.file_path and result.file_path.exists():
                try:
                    result.file_path.unlink()
                    logger.info(f"Cleaned up failed file: {result.file_path}")
                except Exception as e:
                    logger.warning(f"Could not clean up file: {e}")
        
        # All strategies failed
        duration = time.time() - start_time
        return DownloadResult(
            success=False,
            strategy_used=None,
            file_path=None,
            file_size=0,
            error="All download strategies failed",
            duration_seconds=duration,
        )
    
    def _try_strategy(
        self,
        strategy: DownloadStrategy,
        url: str,
        job_id: str
    ) -> DownloadResult:
        """Try a specific download strategy."""
        start_time = time.time()
        output_path = self.output_dir / f"{job_id}.mp4"
        
        last_error: Optional[str] = None

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Trying {strategy.value} (attempt {attempt + 1}/{self.max_retries})")
                
                if strategy == DownloadStrategy.YT_DLP_COOKIES:
                    result = self._download_ytdlp(url, output_path, use_cookies=True)
                elif strategy == DownloadStrategy.YT_DLP_BROWSER:
                    result = self._download_ytdlp_browser(url, output_path)
                elif strategy == DownloadStrategy.YT_DLP_DIRECT:
                    result = self._download_ytdlp(url, output_path, use_cookies=False)
                elif strategy == DownloadStrategy.DIRECT_URL:
                    result = self._download_direct(url, output_path)
                else:
                    return DownloadResult(
                        success=False,
                        strategy_used=strategy,
                        file_path=output_path,
                        file_size=0,
                        error=f"Unknown strategy: {strategy}",
                        duration_seconds=time.time() - start_time,
                    )
                
                if result.success and result.file_size > 0:
                    return result

                last_error = result.error
                
                # Retry on failure
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"Exception during download: {e}")
                return DownloadResult(
                    success=False,
                    strategy_used=strategy,
                    file_path=output_path,
                    file_size=output_path.stat().st_size if output_path.exists() else 0,
                    error=str(e),
                    duration_seconds=time.time() - start_time,
                )
        
        return DownloadResult(
            success=False,
            strategy_used=strategy,
            file_path=output_path,
            file_size=output_path.stat().st_size if output_path.exists() else 0,
            error=f"All {self.max_retries} attempts failed. Last error: {last_error or 'unknown'}",
            duration_seconds=time.time() - start_time,
        )
    
    def _download_ytdlp(
        self,
        url: str,
        output_path: Path,
        use_cookies: bool = False
    ) -> DownloadResult:
        """Download using yt-dlp."""
        start_time = time.time()
        opts = self._get_ytdlp_opts(output_path, use_cookies=use_cookies)
        
        try:
            # Try Python module first
            import yt_dlp
            
            logger.info(f"Downloading with yt-dlp (cookies={use_cookies})")
            
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
                
                # Get metadata
                metadata = {
                    "title": info.get("title", ""),
                    "duration": info.get("duration", 0),
                    "uploader": info.get("uploader", ""),
                }
                
        except ImportError:
            # Fall back to CLI
            if not self.yt_dlp_path:
                return DownloadResult(
                    success=False,
                    strategy_used=DownloadStrategy.YT_DLP_DIRECT,
                    file_path=output_path,
                    file_size=0,
                    error="yt-dlp not available",
                    duration_seconds=time.time() - start_time,
                )
            
            cmd = [str(self.yt_dlp_path)]
            
            if use_cookies and self.cookies_file and self.cookies_file.exists():
                cmd.extend(["--cookies", str(self.cookies_file)])
            
            cmd.extend([
                "-f", opts.get("format", "best"),
                "-o", str(output_path),
                url
            ])
            
            logger.info(f"Running: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )
            
            if result.returncode != 0:
                return DownloadResult(
                    success=False,
                    strategy_used=DownloadStrategy.YT_DLP_DIRECT,
                    file_path=output_path,
                    file_size=output_path.stat().st_size if output_path.exists() else 0,
                    error=result.stderr[:500],
                    duration_seconds=time.time() - start_time,
                )
            
            # Ensure .mp4 extension for CLI download
            if output_path.suffix.lower() != '.mp4':
                mp4_path = output_path.with_suffix('.mp4')
                output_path.rename(mp4_path)
                output_path = mp4_path
                logger.info(f"Renamed file to: {output_path.name}")
            
            metadata = {}
        
        # Check output file
        if not output_path.exists():
            return DownloadResult(
                success=False,
                strategy_used=DownloadStrategy.YT_DLP_DIRECT,
                file_path=output_path,
                file_size=0,
                error="Output file not created",
                duration_seconds=time.time() - start_time,
            )
        
        # Ensure .mp4 extension - yt-dlp sometimes downloads without extension
        if output_path.suffix.lower() != '.mp4':
            mp4_path = output_path.with_suffix('.mp4')
            output_path.rename(mp4_path)
            output_path = mp4_path
            logger.info(f"Renamed file to: {output_path.name}")
        
        return DownloadResult(
            success=True,
            strategy_used=DownloadStrategy.YT_DLP_DIRECT,
            file_path=output_path,
            file_size=output_path.stat().st_size,
            error=None,
            duration_seconds=time.time() - start_time,
            metadata=metadata,
        )
    
    def _download_ytdlp_browser(self, url: str, output_path: Path) -> DownloadResult:
        """Download using yt-dlp with browser cookies."""
        if not self.yt_dlp_path:
            return DownloadResult(
                success=False,
                strategy_used=DownloadStrategy.YT_DLP_BROWSER,
                file_path=output_path,
                file_size=0,
                error="yt-dlp CLI not available for browser cookies",
                duration_seconds=0,
            )
        
        start_time = time.time()
        
        # Try browser cookies
        for browser in ["chrome", "edge", "firefox"]:
            cmd = [
                str(self.yt_dlp_path),
                "--cookies-from-browser", browser,
                "-f", "best",
                "-o", str(output_path),
                url
            ]
            
            logger.info(f"Trying browser cookies: {browser}")
            
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.timeout
                )
                
                if result.returncode == 0 and output_path.exists():
                    # Ensure .mp4 extension
                    if output_path.suffix.lower() != '.mp4':
                        mp4_path = output_path.with_suffix('.mp4')
                        output_path.rename(mp4_path)
                        output_path = mp4_path
                        logger.info(f"Renamed file to: {output_path.name}")
                    
                    return DownloadResult(
                        success=True,
                        strategy_used=DownloadStrategy.YT_DLP_BROWSER,
                        file_path=output_path,
                        file_size=output_path.stat().st_size,
                        error=None,
                        duration_seconds=time.time() - start_time,
                        metadata={"browser": browser},
                    )
            except Exception as e:
                logger.warning(f"Browser {browser} failed: {e}")
                continue
        
        return DownloadResult(
            success=False,
            strategy_used=DownloadStrategy.YT_DLP_BROWSER,
            file_path=output_path,
            file_size=output_path.stat().st_size if output_path.exists() else 0,
            error="All browser cookie sources failed",
            duration_seconds=time.time() - start_time,
        )
    
    def _download_direct(self, url: str, output_path: Path) -> DownloadResult:
        """
        Direct URL extraction and download fallback.
        
        Extracts video URL from page HTML and downloads directly.
        """
        start_time = time.time()
        
        try:
            # Get page HTML
            headers = {
                "User-Agent": self.USER_AGENTS[0],
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            }
            
            # Add platform-specific headers
            if self.platform == Platform.TIKTOK:
                headers["Referer"] = "https://www.tiktok.com/"
            
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            
            html = response.text
            
            # Extract video URL based on platform
            video_url = self._extract_video_url(html, url)
            
            if not video_url:
                return DownloadResult(
                    success=False,
                    strategy_used=DownloadStrategy.DIRECT_URL,
                    file_path=output_path,
                    file_size=0,
                    error="Could not extract video URL from page",
                    duration_seconds=time.time() - start_time,
                )
            
            logger.info(f"Extracted video URL: {video_url[:100]}...")
            
            # Download video
            video_response = requests.get(video_url, headers=headers, stream=True, timeout=self.timeout)
            video_response.raise_for_status()
            
            total_size = int(video_response.headers.get("Content-Length", 0))
            
            with open(output_path, "wb") as f:
                for chunk in video_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return DownloadResult(
                success=True,
                strategy_used=DownloadStrategy.DIRECT_URL,
                file_path=output_path,
                file_size=output_path.stat().st_size,
                error=None,
                duration_seconds=time.time() - start_time,
                metadata={"source_url": video_url[:200]},
            )
            
        except requests.RequestException as e:
            return DownloadResult(
                success=False,
                strategy_used=DownloadStrategy.DIRECT_URL,
                file_path=output_path,
                file_size=output_path.stat().st_size if output_path.exists() else 0,
                error=f"Request failed: {e}",
                duration_seconds=time.time() - start_time,
            )
        except Exception as e:
            return DownloadResult(
                success=False,
                strategy_used=DownloadStrategy.DIRECT_URL,
                file_path=output_path,
                file_size=output_path.stat().st_size if output_path.exists() else 0,
                error=str(e),
                duration_seconds=time.time() - start_time,
            )
    
    def _extract_video_url(self, html: str, original_url: str) -> Optional[str]:
        """Extract video URL from HTML based on platform."""
        
        if self.platform == Platform.TIKTOK:
            # TikTok video URL patterns
            patterns = [
                r'"playAddr":"([^"]+)"',
                r'"src":"([^"]+)"',
                r'https?://[^"]+\.mp4[^"]*',
            ]
            for pattern in patterns:
                match = re.search(pattern, html)
                if match:
                    url = match.group(1)
                    if url.startswith("//"):
                        url = "https:" + url
                    return self._normalize_extracted_url(url)
        
        elif self.platform == Platform.YOUTUBE:
            # YouTube streaming URL patterns
            patterns = [
                r'"streamingData":\{[^}]*"formats":\[[^\]]*"url":"([^"]+)"',
                r'"adaptiveFormats":\[[^\]]*"url":"([^"]+)"',
            ]
            for pattern in patterns:
                match = re.search(pattern, html)
                if match:
                    return self._normalize_extracted_url(match.group(1))
        
        elif self.platform == Platform.TWITTER:
            # Twitter video URL patterns
            patterns = [
                r'"bitrate":\d+,"content_type":"video/mp4","url":"([^"]+)"',
                r'https?://[^"]+\.mp4\?tag=\d+',
            ]
            for pattern in patterns:
                match = re.search(pattern, html)
                if match:
                    url = match.group(1)
                    if url.startswith("//"):
                        url = "https:" + url
                    return self._normalize_extracted_url(url)
        
        # Generic MP4 URL extraction
        mp4_pattern = r'(https?://[^\s"\'<>]+\.mp4(?:[^\s"\'<>]*)?)'
        match = re.search(mp4_pattern, html)
        if match:
            return self._normalize_extracted_url(match.group(1))
        
        return None
