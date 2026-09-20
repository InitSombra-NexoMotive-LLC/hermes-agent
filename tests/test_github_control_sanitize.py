import pytest
from gateway.github_control_sanitize import sanitize_result,MAX_RESULT_CHARS

def test_normal_structured_empty_and_digest_stable():
 assert sanitize_result(None)['summary']=='None'
 assert sanitize_result({'a':1})['summary']=='{"a": 1}'
 assert sanitize_result('x')['digest']==sanitize_result('x')['digest']
def test_secret_and_routing_redaction():
 s=sanitize_result('Bearer abc\nTOKEN=xyz\nagent:main:telegram:dm:123')
 assert 'abc' not in s['summary'] and 'xyz' not in s['summary'] and '123' not in s['summary']
def test_private_key_and_unsupported_fail_closed():
 with pytest.raises(ValueError):sanitize_result('-----BEGIN PRIVATE KEY-----')
 with pytest.raises(ValueError):sanitize_result(object())
def test_controls_and_truncation():
 s=sanitize_result('a\x00b\n'+('x'*(MAX_RESULT_CHARS+1)))
 assert s['truncated'] and '\x00' not in s['summary'] and len(s['summary'])==MAX_RESULT_CHARS
