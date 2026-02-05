"""SEC EDGAR filing downloader pipeline with rate limiting."""
from sec_edgar_downloader import Downloader
from pathlib import Path
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class SECEdgarPipeline:
    """Pipeline for downloading SEC filings with rate limiting."""

    # SEC EDGAR rate limit: 10 requests per second
    MAX_REQUESTS_PER_SECOND = 10
    MIN_REQUEST_INTERVAL = 1.0 / MAX_REQUESTS_PER_SECOND  # 0.1 seconds

    def __init__(
        self,
        company_name: str,
        email: str,
        download_dir: Path = Path("data/raw/sec"),
        rate_limit_buffer: float = 0.05  # Extra 50ms buffer for safety
    ):
        """
        Initialize SEC EDGAR downloader with rate limiting.

        Args:
            company_name: Your company/organization name
            email: Your email address (required by SEC)
            download_dir: Directory to save downloaded filings
            rate_limit_buffer: Extra delay buffer (seconds) to stay under rate limit
        """
        self.dl = Downloader(company_name, email, download_dir)
        self.download_dir = download_dir
        self.rate_limit_buffer = rate_limit_buffer
        self.last_request_time: Optional[float] = None
        self.request_count = 0
        self.rate_limit_hits = 0

    def _wait_for_rate_limit(self):
        """Wait if necessary to respect SEC rate limits."""
        if self.last_request_time is None:
            self.last_request_time = time.time()
            return

        # Calculate time since last request
        elapsed = time.time() - self.last_request_time
        required_interval = self.MIN_REQUEST_INTERVAL + self.rate_limit_buffer

        # Wait if needed
        if elapsed < required_interval:
            wait_time = required_interval - elapsed
            logger.debug(f"Rate limit: waiting {wait_time:.3f}s")
            time.sleep(wait_time)

        self.last_request_time = time.time()

    def _retry_with_backoff(
        self,
        func,
        max_retries: int = 3,
        initial_wait: float = 2.0
    ):
        """
        Retry a function with exponential backoff.

        Args:
            func: Function to retry
            max_retries: Maximum number of retry attempts
            initial_wait: Initial wait time in seconds

        Returns:
            Function result or raises exception
        """
        wait_time = initial_wait

        for attempt in range(max_retries):
            try:
                self._wait_for_rate_limit()
                result = func()
                self.request_count += 1
                return result

            except Exception as e:
                error_msg = str(e).lower()

                # Check if it's a rate limit error
                if "429" in error_msg or "rate limit" in error_msg:
                    self.rate_limit_hits += 1
                    logger.warning(
                        f"Rate limit hit (attempt {attempt + 1}/{max_retries}). "
                        f"Waiting {wait_time:.1f}s before retry..."
                    )
                    time.sleep(wait_time)
                    wait_time *= 2  # Exponential backoff
                    continue

                # For other errors, retry with shorter wait
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Error on attempt {attempt + 1}/{max_retries}: {e}. "
                        f"Retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    wait_time *= 1.5
                    continue

                # Max retries reached
                raise

        raise Exception(f"Max retries ({max_retries}) exceeded")

    def download_filings(
        self,
        ticker: str,
        filing_types: list[str] = ["10-K", "10-Q", "8-K"],
        limit: int = 10,
        after: str = "2020-01-01",
        max_retries: int = 3
    ) -> list[Path]:
        """
        Download filings for a company with rate limiting and retry logic.

        Args:
            ticker: Company ticker symbol
            filing_types: List of filing types to download
            limit: Maximum filings per type
            after: Only filings after this date (YYYY-MM-DD)
            max_retries: Maximum retry attempts per filing type

        Returns:
            List of paths to downloaded filings
        """
        downloaded = []
        start_time = time.time()

        logger.info(
            f"Starting download for {ticker}: "
            f"{len(filing_types)} filing types, limit={limit}, after={after}"
        )

        for idx, filing_type in enumerate(filing_types, 1):
            try:
                logger.info(
                    f"[{idx}/{len(filing_types)}] Downloading {filing_type} for {ticker}..."
                )

                # Download with retry logic
                def download_func():
                    return self.dl.get(
                        filing_type,
                        ticker,
                        limit=limit,
                        after=after
                    )

                self._retry_with_backoff(download_func, max_retries=max_retries)

                # Find downloaded files
                filing_dir = self.download_dir / "sec-edgar-filings" / ticker / filing_type
                if filing_dir.exists():
                    filing_paths = list(filing_dir.glob("**/full-submission.txt"))
                    downloaded.extend(filing_paths)
                    logger.info(f"✓ Downloaded {len(filing_paths)} {filing_type} filings")
                else:
                    logger.warning(f"No files found for {filing_type}")

            except Exception as e:
                logger.error(f"✗ Failed to download {filing_type} for {ticker}: {e}")
                continue

        elapsed = time.time() - start_time
        logger.info(
            f"Download complete for {ticker}: "
            f"{len(downloaded)} files in {elapsed:.1f}s "
            f"({self.request_count} requests, {self.rate_limit_hits} rate limit hits)"
        )

        return downloaded

    def download_batch(
        self,
        tickers: list[str],
        filing_types: list[str] = ["10-K", "10-Q", "8-K"],
        limit: int = 10,
        after: str = "2020-01-01",
        delay_between_tickers: float = 1.0
    ) -> dict[str, list[Path]]:
        """
        Download filings for multiple companies with rate limiting.

        Args:
            tickers: List of company ticker symbols
            filing_types: List of filing types to download
            limit: Maximum filings per type per ticker
            after: Only filings after this date (YYYY-MM-DD)
            delay_between_tickers: Extra delay between tickers (seconds)

        Returns:
            Dictionary mapping ticker to list of downloaded file paths
        """
        results = {}
        start_time = time.time()

        logger.info(f"Starting batch download for {len(tickers)} tickers")

        for idx, ticker in enumerate(tickers, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing ticker {idx}/{len(tickers)}: {ticker}")
            logger.info(f"{'='*60}")

            try:
                files = self.download_filings(
                    ticker=ticker,
                    filing_types=filing_types,
                    limit=limit,
                    after=after
                )
                results[ticker] = files

                # Extra delay between tickers
                if idx < len(tickers):
                    logger.info(f"Waiting {delay_between_tickers}s before next ticker...")
                    time.sleep(delay_between_tickers)

            except Exception as e:
                logger.error(f"Failed to process {ticker}: {e}")
                results[ticker] = []
                continue

        elapsed = time.time() - start_time
        total_files = sum(len(files) for files in results.values())

        logger.info(f"\n{'='*60}")
        logger.info(f"Batch download complete:")
        logger.info(f"  - Total tickers: {len(tickers)}")
        logger.info(f"  - Total files: {total_files}")
        logger.info(f"  - Total time: {elapsed:.1f}s")
        logger.info(f"  - Total requests: {self.request_count}")
        logger.info(f"  - Rate limit hits: {self.rate_limit_hits}")
        logger.info(f"{'='*60}")

        return results

    def get_stats(self) -> dict:
        """Get download statistics."""
        return {
            "total_requests": self.request_count,
            "rate_limit_hits": self.rate_limit_hits,
            "avg_request_interval": self.MIN_REQUEST_INTERVAL + self.rate_limit_buffer
        }
