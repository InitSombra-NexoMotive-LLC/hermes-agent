import json
from gateway.github_control_ledger import CommandLedger

def command_status_response(request, ledger):
    command_id=request.get('command_id') if isinstance(request,dict) else None
    import re
    if not isinstance(command_id,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',command_id): return {'status':'INVALID'}
    value=ledger.status(command_id)
    return {'status':'NOT_FOUND'} if value is None else {'status':'OK',**value}

def test_status_invalid_unknown_and_sanitized_complete(tmp_path):
    ledger=CommandLedger(tmp_path/'x.db')
    assert command_status_response({},ledger)['status']=='INVALID'
    assert command_status_response({'command_id':'bad space'},ledger)['status']=='INVALID'
    assert command_status_response({'command_id':'none'},ledger)['status']=='NOT_FOUND'
    p={'command_id':'ok','worker_id':'nvidia-control','workstream':'CONTROL_PLANE'};ledger.accept(p);ledger.transition('ok','QUEUED',('ACCEPTED',));ledger.transition('ok','RUNNING',('QUEUED',));ledger.transition('ok','DELIVERED',('RUNNING',));ledger.complete('ok',{'summary':'safe','digest':'d','truncated':False})
    out=command_status_response({'command_id':'ok'},ledger)
    assert out['sanitized_result']=='safe' and 'payload' not in out and 'instructions' not in out
