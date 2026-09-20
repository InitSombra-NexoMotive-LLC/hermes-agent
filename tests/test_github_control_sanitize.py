import pytest
from gateway.github_control_sanitize import sanitize_result,MAX_RESULT_CHARS

def test_normal_structured_empty_and_digest_stable():
 assert sanitize_result(None)['summary']=='None'
 assert sanitize_result({'a':1})['summary']=='{"a": 1}'
 assert sanitize_result('x')['digest']==sanitize_result('x')['digest']
def test_secret_and_routing_redaction():
 s=sanitize_result('Bearer abc\nTOKEN=xyz\nagent:main:telegram:dm:123')
 assert 'abc' not in s['summary'] and 'xyz' not in s['summary'] and '123' not in s['summary']
@pytest.mark.parametrize('value',[
 '-----BEGIN PRIVATE KEY-----',
 '-----BEGIN RSA PRIVATE KEY-----',
 '-----BEGIN OPENSSH PRIVATE KEY-----',
 '-----BEGIN PRI\x00VATE KEY-----',
 '-----BEGIN\x00 PRIVATE KEY-----',
])
def test_private_key_variants_fail_closed_without_echo(value):
 with pytest.raises(ValueError) as raised: sanitize_result(value)
 assert str(raised.value)=='PRIVATE_KEY_REJECTED'
 assert value not in str(raised.value)

def test_unsupported_top_level_fails_closed():
 with pytest.raises(ValueError) as raised: sanitize_result(object())
 assert str(raised.value)=='UNSUPPORTED_RESULT'

@pytest.mark.parametrize('value',[{'nested':object()},[object()]])
def test_nested_unsupported_result_fails_closed_without_echo(value):
 with pytest.raises(ValueError) as raised: sanitize_result(value)
 assert str(raised.value)=='UNSUPPORTED_RESULT'
 assert repr(value) not in str(raised.value)
def test_controls_and_truncation():
 s=sanitize_result('a\x00b\n'+('x'*(MAX_RESULT_CHARS+1)))
 assert s['truncated'] and '\x00' not in s['summary'] and len(s['summary'])==MAX_RESULT_CHARS
