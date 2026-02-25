import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'ingestion'))
from main_ingestion import (
    get_target_months_from_june_2025,
    get_missing_months,
    get_uploaded_months,
    upload_to_gcs,
    download_and_upload,
    ingest_next_months,
    GCS_BUCKET_NAME,
    GCS_PREFIX,
    BASE_URL
)


class TestGetTargetMonthsFromJune2025:
    """Tests for get_target_months_from_june_2025 function."""
    
    def test_returns_list_starting_from_june_2025(self):
        """Should return list starting with 2025-06."""
        result = get_target_months_from_june_2025()
        assert result[0] == "2025-06"
    
    def test_returns_list_of_strings(self):
        """Should return list of strings in YYYY-MM format."""
        result = get_target_months_from_june_2025()
        assert all(isinstance(m, str) for m in result)
        assert all(len(m) == 7 for m in result)
        assert all('-' in m for m in result)
    
    def test_months_are_chronological(self):
        """Should return months in chronological order."""
        result = get_target_months_from_june_2025()
        assert result == sorted(result)
    
    def test_contains_at_least_six_months(self):
        """Should contain at least 6 months (Jun-Nov 2025)."""
        result = get_target_months_from_june_2025()
        assert len(result) >= 6


class TestGetMissingMonths:
    """Tests for get_missing_months function."""
    
    def test_returns_missing_months(self):
        """Should return months not in uploaded list."""
        target = ["2025-06", "2025-07", "2025-08"]
        uploaded = ["2025-06"]
        result = get_missing_months(target, uploaded)
        assert result == ["2025-07", "2025-08"]
    
    def test_returns_empty_if_all_uploaded(self):
        """Should return empty list if all months are uploaded."""
        target = ["2025-06", "2025-07"]
        uploaded = ["2025-06", "2025-07"]
        result = get_missing_months(target, uploaded)
        assert result == []
    
    def test_returns_all_if_none_uploaded(self):
        """Should return all target months if none are uploaded."""
        target = ["2025-06", "2025-07"]
        uploaded = []
        result = get_missing_months(target, uploaded)
        assert result == target
    
    def test_ignores_extra_uploaded_months(self):
        """Should ignore uploaded months not in target list."""
        target = ["2025-06", "2025-07"]
        uploaded = ["2025-06", "2024-01", "2024-02"]
        result = get_missing_months(target, uploaded)
        assert result == ["2025-07"]


class TestGetUploadedMonths:
    """Tests for get_uploaded_months function."""
    
    @patch('main_ingestion.get_gcs_client')
    def test_returns_empty_list_when_bucket_empty(self, mock_get_client):
        """Should return empty list when no blobs exist."""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_bucket.list_blobs.return_value = []
        mock_client.bucket.return_value = mock_bucket
        mock_get_client.return_value = mock_client
        
        result = get_uploaded_months(mock_client)
        assert result == []
    
    @patch('main_ingestion.get_gcs_client')
    def test_extracts_months_from_blob_names(self, mock_get_client):
        """Should extract YYYY-MM from blob names matching pattern."""
        mock_client = Mock()
        mock_bucket = Mock()
        
        mock_blob1 = Mock()
        mock_blob1.name = "yellow_taxi/2025-06/yellow_tripdata.parquet"
        mock_blob2 = Mock()
        mock_blob2.name = "yellow_taxi/2025-07/yellow_tripdata.parquet"
        mock_blob3 = Mock()
        mock_blob3.name = "yellow_taxi/2025-08/yellow_tripdata.parquet"
        
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        mock_client.bucket.return_value = mock_bucket
        mock_get_client.return_value = mock_client
        
        result = get_uploaded_months(mock_client)
        assert result == ["2025-06", "2025-07", "2025-08"]
    
    @patch('main_ingestion.get_gcs_client')
    def test_ignores_non_matching_blobs(self, mock_get_client):
        """Should ignore blobs that don't match the expected pattern."""
        mock_client = Mock()
        mock_bucket = Mock()
        
        mock_blob1 = Mock()
        mock_blob1.name = "yellow_taxi/2025-06/yellow_tripdata.parquet"
        mock_blob2 = Mock()
        mock_blob2.name = "other_folder/2025-07/data.parquet"
        mock_blob3 = Mock()
        mock_blob3.name = "yellow_taxi/invalid_format.parquet"
        
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        mock_client.bucket.return_value = mock_bucket
        mock_get_client.return_value = mock_client
        
        result = get_uploaded_months(mock_client)
        assert result == ["2025-06"]
    
    @patch('main_ingestion.get_gcs_client')
    def test_returns_sorted_months(self, mock_get_client):
        """Should return months in sorted order."""
        mock_client = Mock()
        mock_bucket = Mock()
        
        mock_blob1 = Mock()
        mock_blob1.name = "yellow_taxi/2025-08/yellow_tripdata.parquet"
        mock_blob2 = Mock()
        mock_blob2.name = "yellow_taxi/2025-06/yellow_tripdata.parquet"
        
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        mock_client.bucket.return_value = mock_bucket
        mock_get_client.return_value = mock_client
        
        result = get_uploaded_months(mock_client)
        assert result == ["2025-06", "2025-08"]


class TestUploadToGcs:
    """Tests for upload_to_gcs function."""
    
    def test_returns_true_on_successful_upload(self):
        """Should return True when upload succeeds."""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        
        result = upload_to_gcs(mock_client, "2025-06", b"test content")
        assert result is True
    
    def test_calls_upload_with_correct_path(self):
        """Should call upload with correct blob path."""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        
        upload_to_gcs(mock_client, "2025-06", b"test content")
        
        expected_path = "yellow_taxi/2025-06/yellow_tripdata.parquet"
        mock_bucket.blob.assert_called_once_with(expected_path)
    
    def test_returns_false_on_exception(self):
        """Should return False when upload fails."""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.upload_from_string.side_effect = Exception("Upload failed")
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        
        result = upload_to_gcs(mock_client, "2025-06", b"test content")
        assert result is False


class TestDownloadAndUpload:
    """Tests for download_and_upload function."""
    
    @patch('main_ingestion.upload_to_gcs')
    @patch('main_ingestion.requests.get')
    @patch('main_ingestion.get_gcs_client')
    def test_returns_true_on_success(self, mock_get_client, mock_get, mock_upload):
        """Should return True when download and upload succeed."""
        mock_response = Mock()
        mock_response.content = b"parquet data"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        mock_upload.return_value = True
        
        result = download_and_upload("2025-06")
        assert result is True
    
    @patch('main_ingestion.requests.get')
    @patch('main_ingestion.get_gcs_client')
    def test_returns_false_on_404(self, mock_get_client, mock_get):
        """Should return False when file returns 404."""
        import requests as req
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = req.exceptions.HTTPError()
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error
        mock_get.return_value = mock_response
        
        result = download_and_upload("2025-06")
        assert result is False
    
    @patch('main_ingestion.requests.get')
    @patch('main_ingestion.get_gcs_client')
    def test_returns_false_on_connection_error(self, mock_get_client, mock_get):
        """Should return False on connection error."""
        import requests as req
        mock_get.side_effect = req.exceptions.RequestException("Connection error")
        
        result = download_and_upload("2025-06")
        assert result is False
    
    @patch('main_ingestion.upload_to_gcs')
    @patch('main_ingestion.requests.get')
    @patch('main_ingestion.get_gcs_client')
    def test_uploads_downloaded_content(self, mock_get_client, mock_get, mock_upload):
        """Should upload the downloaded content to GCS."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        mock_response = Mock()
        mock_response.content = b"parquet data"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        mock_upload.return_value = True
        
        download_and_upload("2025-06", mock_client)
        
        mock_upload.assert_called_once_with(mock_client, "2025-06", b"parquet data")


class TestIngestNextMonths:
    """Tests for ingest_next_months function."""
    
    @patch('main_ingestion.download_and_upload')
    @patch('main_ingestion.get_missing_months')
    @patch('main_ingestion.get_target_months_from_june_2025')
    @patch('main_ingestion.get_uploaded_months')
    @patch('main_ingestion.get_gcs_client')
    def test_processes_n_months(self, mock_get_client, mock_uploaded, mock_target, mock_missing, mock_download):
        """Should process exactly n missing months."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_uploaded.return_value = []
        mock_target.return_value = ["2025-06", "2025-07", "2025-08", "2025-09"]
        mock_missing.return_value = ["2025-06", "2025-07", "2025-08", "2025-09"]
        mock_download.return_value = True
        
        result = ingest_next_months(n=2)
        
        assert mock_download.call_count == 2
        assert len(result) == 2
    
    @patch('main_ingestion.get_missing_months')
    @patch('main_ingestion.get_target_months_from_june_2025')
    @patch('main_ingestion.get_uploaded_months')
    @patch('main_ingestion.get_gcs_client')
    def test_returns_empty_if_no_missing(self, mock_get_client, mock_uploaded, mock_target, mock_missing):
        """Should return empty list if no months are missing."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_uploaded.return_value = ["2025-06", "2025-07"]
        mock_target.return_value = ["2025-06", "2025-07"]
        mock_missing.return_value = []
        
        result = ingest_next_months(n=3)
        
        assert result == []
    
    @patch('main_ingestion.download_and_upload')
    @patch('main_ingestion.get_missing_months')
    @patch('main_ingestion.get_target_months_from_june_2025')
    @patch('main_ingestion.get_uploaded_months')
    @patch('main_ingestion.get_gcs_client')
    def test_only_returns_successful_downloads(self, mock_get_client, mock_uploaded, mock_target, mock_missing, mock_download):
        """Should only include successfully processed months in result."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_uploaded.return_value = []
        mock_target.return_value = ["2025-06", "2025-07", "2025-08"]
        mock_missing.return_value = ["2025-06", "2025-07", "2025-08"]
        mock_download.side_effect = [True, False, True]
        
        result = ingest_next_months(n=3)
        
        assert result == ["2025-06", "2025-08"]


class TestConstants:
    """Tests for module constants."""
    
    def test_gcs_bucket_name(self):
        """Should have correct bucket name."""
        assert GCS_BUCKET_NAME == "nyc-taxi-mini-landing"
    
    def test_gcs_prefix(self):
        """Should have correct GCS prefix."""
        assert GCS_PREFIX == "yellow_taxi/"
    
    def test_base_url_format(self):
        """Should have correct base URL format."""
        assert "yellow_tripdata_{date}.parquet" in BASE_URL
