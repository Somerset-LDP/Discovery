"""
Tests for Patient Linking Service.
"""
from datetime import date, timedelta
import pytest

from linking.service import LinkageService, Sex

# NHS number validation tests

def test_link_with_valid_nhs_number_with_spaces():
    """Test valid NHS number with spaces is accepted."""
    service = LinkageService()
    result = service.link(
        nhs_number="943 476 5919",
        dob=date(1980, 5, 15)
    )
    assert result is None


def test_link_with_invalid_nhs_number_checksum():
    """Test NHS number with invalid check digit raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            nhs_number="9434765910",
            dob=date(1980, 5, 15)
        )
    assert "Invalid NHS number format" in str(exc_info.value)


def test_link_with_nhs_number_too_short():
    """Test NHS number with fewer than 10 digits raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            nhs_number="943476591",
            dob=date(1980, 5, 15)
        )
    assert "Invalid NHS number format" in str(exc_info.value)


def test_link_with_nhs_number_too_long():
    """Test NHS number with more than 10 digits raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            nhs_number="94347659199",
            dob=date(1980, 5, 15)
        )
    assert "Invalid NHS number format" in str(exc_info.value)


def test_link_with_non_numeric_nhs_number():
    """Test NHS number with non-numeric characters raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            nhs_number="abcdefghij",
            dob=date(1980, 5, 15)
        )
    assert "Invalid NHS number format" in str(exc_info.value)


def test_link_with_empty_nhs_number():
    """Test empty string NHS number raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            nhs_number="",
            dob=date(1980, 5, 15)
        )
    assert "Invalid NHS number format" in str(exc_info.value)


# Postcode validation tests

@pytest.mark.parametrize("postcode", [
    "SW1A 1AA",
    "M1 1AA",
    "TA1 1AA",
    "BS1 1AA",
    "SW1A1AA",  # without space
    " M1 1AA ",  # with whitespace
])
def test_link_with_valid_postcodes(postcode):
    """Test various valid UK postcode formats are accepted."""
    service = LinkageService()
    result = service.link(
        first_name="John",
        last_name="Smith",
        sex=Sex.MALE,
        postcode=postcode,
        dob=date(1980, 5, 15)
    )
    assert result is None


def test_link_with_invalid_postcode_format():
    """Test invalid postcode format raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            first_name="John",
            last_name="Smith",
            sex=Sex.MALE,
            postcode="INVALID",
            dob=date(1980, 5, 15)
        )
    assert "Invalid UK postcode format" in str(exc_info.value)


def test_link_with_empty_postcode():
    """Test empty string postcode raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            first_name="John",
            last_name="Smith",
            sex=Sex.MALE,
            postcode="",
            dob=date(1980, 5, 15)
        )
    assert "Invalid UK postcode format" in str(exc_info.value)


# Name validation tests

def test_link_with_valid_names():
    """Test valid names are accepted with full demographics."""
    service = LinkageService()
    result = service.link(
        first_name="John",
        last_name="Smith",
        sex=Sex.MALE,
        postcode="SW1A 1AA",
        dob=date(1980, 5, 15)
    )
    assert result is None


def test_link_with_multi_word_names():
    """Test multi-word names are accepted."""
    service = LinkageService()
    result = service.link(
        first_name="Mary Jane",
        last_name="Smith-Jones",
        sex=Sex.FEMALE,
        postcode="SW1A 1AA",
        dob=date(1980, 5, 15)
    )
    assert result is None


def test_link_with_empty_first_name():
    """Test empty first name raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            first_name="",
            last_name="Smith",
            sex=Sex.MALE,
            postcode="SW1A 1AA",
            dob=date(1980, 5, 15)
        )
    assert "First name cannot be empty or whitespace" in str(exc_info.value)


def test_link_with_whitespace_first_name():
    """Test whitespace-only first name raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            first_name="   ",
            last_name="Smith",
            sex=Sex.MALE,
            postcode="SW1A 1AA",
            dob=date(1980, 5, 15)
        )
    assert "First name cannot be empty or whitespace" in str(exc_info.value)


def test_link_with_empty_last_name():
    """Test empty last name raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            first_name="John",
            last_name="",
            sex=Sex.MALE,
            postcode="SW1A 1AA",
            dob=date(1980, 5, 15)
        )
    assert "Last name cannot be empty or whitespace" in str(exc_info.value)


def test_link_with_whitespace_last_name():
    """Test whitespace-only last name raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            first_name="John",
            last_name="   ",
            sex=Sex.MALE,
            postcode="SW1A 1AA",
            dob=date(1980, 5, 15)
        )
    assert "Last name cannot be empty or whitespace" in str(exc_info.value)


# Date of birth validation tests

def test_link_with_past_date_of_birth():
    """Test date in the past is accepted."""
    service = LinkageService()
    result = service.link(
        nhs_number="9434765919",
        dob=date(1980, 5, 15)
    )
    assert result is None


def test_link_with_today_date_of_birth():
    """Test today's date is accepted."""
    service = LinkageService()
    result = service.link(
        nhs_number="9434765919",
        dob=date.today()
    )
    assert result is None


def test_link_with_future_date_of_birth():
    """Test date in the future raises ValueError."""
    service = LinkageService()
    future_date = date.today() + timedelta(days=365)
    with pytest.raises(ValueError) as exc_info:
        service.link(
            nhs_number="9434765919",
            dob=future_date
        )
    assert "Date of birth cannot be in the future" in str(exc_info.value)


def test_link_with_tomorrow_date_of_birth():
    """Test tomorrow's date raises ValueError."""
    service = LinkageService()
    tomorrow = date.today() + timedelta(days=1)
    with pytest.raises(ValueError) as exc_info:
        service.link(
            nhs_number="9434765919",
            dob=tomorrow
        )
    assert "Date of birth cannot be in the future" in str(exc_info.value)


# Minimum parameter requirements tests

def test_link_with_no_parameters():
    """Test calling link with no parameters raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link()
    assert "Insufficient data for patient linking" in str(exc_info.value)


def test_link_with_partial_demographics_without_nhs():
    """Test partial demographic data without NHS number raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(
            first_name="John",
            dob=date(1980, 5, 15)
        )
    assert "Insufficient data for patient linking" in str(exc_info.value)


def test_link_with_only_postcode():
    """Test providing only postcode raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(postcode="SW1A 1AA")
    assert "Insufficient data for patient linking" in str(exc_info.value)


def test_link_with_only_sex():
    """Test providing only sex raises ValueError."""
    service = LinkageService()
    with pytest.raises(ValueError) as exc_info:
        service.link(sex=Sex.MALE)
    assert "Insufficient data for patient linking" in str(exc_info.value)


def test_link_with_valid_nhs_number_and_dob():
    """Test that the method successfully proceeds when provided with NHS number and DOB."""
    service = LinkageService()
    
    # Should not raise an exception
    result = service.link(
        nhs_number="9434765919",
        dob=date(1980, 5, 15)
    )
    
    # Assert method completes without error
    # Note: Currently returns None as implementation is pending
    assert result is None


def test_link_with_valid_full_demographic_set_without_nhs_number():
    """Test that the method successfully proceeds with all demographic attributes but no NHS number."""
    service = LinkageService()
    
    # Should not raise an exception
    result = service.link(
        first_name="John",
        last_name="Smith",
        sex=Sex.MALE,
        dob=date(1980, 5, 15),
        postcode="TA1 1AA"
    )
    
    # Assert method completes without error
    assert result is None


def test_link_with_nhs_number_without_dob_raises_value_error():
    """Test that providing NHS number without DOB raises ValueError."""
    service = LinkageService()
    
    with pytest.raises(ValueError) as exc_info:
        service.link(nhs_number="9434765919")
    
    assert "Insufficient data for patient linking" in str(exc_info.value)