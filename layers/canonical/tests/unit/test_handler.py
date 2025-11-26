from aws.lambdas.handler import _validate_event


class TestEventValidation:
    """Tests for event validation"""

    def test_validate_event_with_valid_gp_event(self):
        """Test validation passes with valid GP event"""
        event = {
            'feed_type': 'gp',
            'input_path': 's3://bucket/path/file.csv'
        }
        
        result = _validate_event(event)
        
        assert result is None

    def test_validate_event_with_valid_sft_event(self):
        """Test validation passes with valid SFT event"""
        event = {
            'feed_type': 'sft',
            'input_path': 's3://bucket/path/file.csv'
        }
        
        result = _validate_event(event)
        
        assert result is None

    def test_validate_event_with_uppercase_feed_type(self):
        """Test validation passes with uppercase feed type"""
        event = {
            'feed_type': 'GP',
            'input_path': 's3://bucket/path/file.csv'
        }
        
        result = _validate_event(event)
        
        assert result is None

    def test_validate_event_with_empty_event(self):
        """Test validation fails with empty event"""
        event = {}
        
        result = _validate_event(event)
        
        assert result == "Event is empty"

    def test_validate_event_with_none_event(self):
        """Test validation fails with None event"""
        result = _validate_event(None)
        
        assert result == "Event is empty"

    def test_validate_event_missing_feed_type(self):
        """Test validation fails when feed_type is missing"""
        event = {
            'input_path': 's3://bucket/path/file.csv'
        }
        
        result = _validate_event(event)
        
        assert result == "Missing required parameter: feed_type"

    def test_validate_event_missing_input_path(self):
        """Test validation fails when input_path is missing"""
        event = {
            'feed_type': 'gp'
        }
        
        result = _validate_event(event)
        
        assert result == "Missing required parameter: input_path"

    def test_validate_event_with_empty_feed_type(self):
        """Test validation fails with empty feed_type"""
        event = {
            'feed_type': '',
            'input_path': 's3://bucket/path/file.csv'
        }
        
        result = _validate_event(event)
        
        assert result == "Invalid feed_type: must be a non-empty string"

    def test_validate_event_with_empty_input_path(self):
        """Test validation fails with empty input_path"""
        event = {
            'feed_type': 'gp',
            'input_path': ''
        }
        
        result = _validate_event(event)
        
        assert result == "Invalid input_path: must be a non-empty string"

    def test_validate_event_with_invalid_feed_type(self):
        """Test validation fails with unsupported feed_type"""
        event = {
            'feed_type': 'invalid_feed',
            'input_path': 's3://bucket/path/file.csv'
        }
        
        result = _validate_event(event)
        
        assert result.startswith("Unsupported feed_type: invalid_feed")
        assert "gp" in result
        assert "sft" in result

