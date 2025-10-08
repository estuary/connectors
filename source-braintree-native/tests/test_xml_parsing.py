"""
Test to verify that our custom braintree_xml_to_dict function produces
identical output to Braintree SDK's XmlUtil.dict_from_xml.
"""

import os
import pytest
from datetime import datetime, date
from braintree.util.xml_util import XmlUtil

from source_braintree_native.api.common import braintree_xml_to_dict


def get_xml_files_from_tests_dir():
    """Get all XML files from the sample_xml_responses directory."""
    tests_dir = os.path.dirname(__file__)
    xml_responses_dir = os.path.join(tests_dir, 'sample_xml_responses')
    xml_files = []

    if os.path.exists(xml_responses_dir):
        for filename in os.listdir(xml_responses_dir):
            if filename.endswith('.xml'):
                filepath = os.path.join(xml_responses_dir, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    xml_files.append((filename, f.read()))

    return xml_files


class TestXmlParsing:
    """Test that our custom XML parser produces identical results to Braintree SDK."""

    @pytest.mark.parametrize("filename,xml_content", get_xml_files_from_tests_dir())
    def test_xml_files_compatibility(self, filename, xml_content):
        """Test that braintree_xml_to_dict produces identical output to XmlUtil.dict_from_xml for the sample XML files."""
        # Parse with Braintree SDK's method
        braintree_result = XmlUtil.dict_from_xml(xml_content)

        # Parse with our custom method
        custom_result = braintree_xml_to_dict(xml_content)

        # They should be identical
        assert custom_result == braintree_result, f"Parsing mismatch in {filename}:\nBraintree SDK: {braintree_result}\nCustom parser: {custom_result}"

    def test_type_conversions(self):
        """Test that type conversions match exactly for valid cases."""
        xml_with_types = """<?xml version="1.0" encoding="UTF-8"?>
        <test-object>
            <string-value>hello world</string-value>
            <integer-value type="integer">42</integer-value>
            <zero-integer type="integer">0</zero-integer>
            <boolean-true type="boolean">true</boolean-true>
            <boolean-false type="boolean">false</boolean-false>
            <boolean-one type="boolean">1</boolean-one>
            <boolean-zero type="boolean">0</boolean-zero>
            <decimal-value type="decimal">123.45</decimal-value>
            <decimal-zero type="decimal">0.00</decimal-zero>
            <datetime-value type="datetime">2023-06-15T10:30:00Z</datetime-value>
            <date-value type="date">2023-06-15</date-value>
            <nil-value nil="true">should be ignored</nil-value>
            <empty-element></empty-element>
        </test-object>"""

        braintree_result = XmlUtil.dict_from_xml(xml_with_types)
        custom_result = braintree_xml_to_dict(xml_with_types)

        assert custom_result == braintree_result

        # Verify specific type conversions
        test_obj = custom_result['test_object']
        assert isinstance(test_obj['integer_value'], int)
        assert test_obj['integer_value'] == 42
        assert isinstance(test_obj['zero_integer'], int)
        assert test_obj['zero_integer'] == 0
        assert isinstance(test_obj['boolean_true'], bool)
        assert test_obj['boolean_true'] is True
        assert isinstance(test_obj['boolean_false'], bool)
        assert test_obj['boolean_false'] is False
        assert isinstance(test_obj['boolean_one'], bool)
        assert test_obj['boolean_one'] is True
        assert isinstance(test_obj['boolean_zero'], bool)
        assert test_obj['boolean_zero'] is False
        assert test_obj['nil_value'] is None
        assert isinstance(test_obj['datetime_value'], datetime)
        assert isinstance(test_obj['date_value'], date)

    def test_array_handling(self):
        """Test that array handling matches exactly."""
        # Empty array
        empty_array_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <root>
            <items type="array"></items>
        </root>"""

        braintree_result = XmlUtil.dict_from_xml(empty_array_xml)
        custom_result = braintree_xml_to_dict(empty_array_xml)
        assert custom_result == braintree_result
        assert custom_result['root']['items'] == []

        # Single item array
        single_array_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <root>
            <items type="array">
                <item>
                    <id>1</id>
                    <name>Single Item</name>
                </item>
            </items>
        </root>"""

        braintree_result = XmlUtil.dict_from_xml(single_array_xml)
        custom_result = braintree_xml_to_dict(single_array_xml)
        assert custom_result == braintree_result
        assert len(custom_result['root']['items']) == 1
        assert isinstance(custom_result['root']['items'], list)

    def test_key_conversion(self):
        """Test that hyphenated keys are converted to underscores."""
        xml_with_hyphens = """<?xml version="1.0" encoding="UTF-8"?>
        <test-object>
            <first-name>John</first-name>
            <last-name>Doe</last-name>
            <created-at type="datetime">2023-01-01T00:00:00Z</created-at>
            <billing-address>
                <street-address>123 Main St</street-address>
                <postal-code>12345</postal-code>
            </billing-address>
        </test-object>"""

        braintree_result = XmlUtil.dict_from_xml(xml_with_hyphens)
        custom_result = braintree_xml_to_dict(xml_with_hyphens)

        assert custom_result == braintree_result

        # Verify key conversion
        test_obj = custom_result['test_object']
        assert 'first_name' in test_obj
        assert 'last_name' in test_obj  
        assert 'created_at' in test_obj
        assert 'billing_address' in test_obj
        assert 'street_address' in test_obj['billing_address']
        assert 'postal_code' in test_obj['billing_address']

    def test_edge_cases(self):
        """Test edge cases that work in both parsers."""
        # XML with nil values
        nil_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <root>
            <nil-with-content nil="true">ignored content</nil-with-content>
            <regular-content>normal value</regular-content>
        </root>"""

        braintree_result = XmlUtil.dict_from_xml(nil_xml)
        custom_result = braintree_xml_to_dict(nil_xml)
        assert custom_result == braintree_result
        assert custom_result['root']['nil_with_content'] is None
