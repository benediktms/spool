import base64, hashlib, json, os, pathlib, sqlite3, subprocess, sys, time
if len(sys.argv) != 4:
 raise SystemExit('usage: capture.py VARIANT ABSOLUTE_BINARY ABSOLUTE_TMP_ROOT')
variant, binary, root_arg = sys.argv[1:]
root = pathlib.Path(root_arg).resolve()
if not root.is_relative_to(pathlib.Path('/tmp').resolve()) or root == pathlib.Path('/tmp').resolve():
 raise SystemExit('root must be a dedicated directory under /tmp')
if not variant or not all(c.isalnum() or c == '-' for c in variant):
 raise SystemExit('variant must contain only letters, digits, or hyphens')
if not pathlib.Path(binary).is_absolute():
 raise SystemExit('binary path must be absolute')
binary_sha256 = hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest()
root.mkdir(parents=True, exist_ok=True)
src = root/'sources'
if src.is_symlink(): raise SystemExit('sources must not be a symlink')
src.mkdir(exist_ok=True)
cases={
'codex_remote_secret':('remote secret debug','Normalize fake credentials before Debug, including remote URLs.'),
'project_migration_trigger':('migration trigger','The migration runs after ingestion, not on an hourly timer.'),
'old_cursors':('old cursors','Old cursors work only for supported format versions.'),
'fingerprint_backfill':('fingerprint backfill','A parser change needs backfill; rediscovery alone is not enough.'),
'claude_cwd_filter':('claude cwd filter','Changing the cwd filter requires a rebuild.'),
'git_discovery_home':('git discovery home','Discovery failed because HOME pointed outside the repository.'),
'read_verbose_cursor':('read verbose cursor','Verbose defaults to false and a continuation preserves it.'),
'long_late_lexical':('rareneedle','The rareneedle correction is the final decision.'),
}
if os.environ.get('SPOOL_COMPACT_HELDOUT') == '1':
 cases['heldout_same_paragraph_utf8'] = ('éclair_unique', 'The setting requires the later qualification.')
for n,(query,answer) in cases.items():
 records=[{'type':'session','version':3,'id':n,'timestamp':'2026-10-12T09:00:00Z','cwd':'/tmp/synthetic','title':n},{'type':'message','id':'a0000001','parentId':None,'timestamp':'2026-10-12T09:00:01Z','message':{'role':'user','content':[{'type':'text','text':(('Background context sentence. '*400)+' '+query if n == 'long_late_lexical' else query+' '+('Background context sentence. '*60))}]}},{'type':'message','id':'a0000002','parentId':'a0000001','timestamp':'2026-10-12T09:00:02Z','message':{'role':'assistant','content':[{'type':'text','text':answer}]}}]
 if n == 'heldout_same_paragraph_utf8':
  records[1]['message']['content'][0]['text'] = query + ' ' + ('café context. '*45) + answer
  records[2]['message']['content'][0]['text'] = 'Acknowledged.'
 data = ''.join(json.dumps(x,separators=(',',':'))+'\n' for x in records)
 path = src/(n+'.jsonl')
 if path.is_symlink(): raise SystemExit(f'fixture must not be a symlink: {path}')
 if path.exists() and path.read_text() != data:
  raise SystemExit(f'frozen fixture differs: {path}')
 if not path.exists(): path.write_text(data)
if {p.name for p in src.iterdir()} != {n+'.jsonl' for n in cases}:
 raise SystemExit('sources contain unexpected files')
oracle = root/'oracle.json'
if oracle.is_symlink(): raise SystemExit('oracle must not be a symlink')
oracle_data = json.dumps(cases, indent=2)
if oracle.exists() and oracle.read_text() != oracle_data:
 raise SystemExit('frozen oracle differs')
if not oracle.exists(): oracle.write_text(oracle_data)
home=root/(variant+'-home'); env={**os.environ,'SPOOL_HOME':str(home)}
if home.exists() or home.is_symlink() or (root/(variant+'.json')).exists() or (root/(variant+'.json')).is_symlink():
 raise SystemExit('variant already exists; use a new variant')
def cli(*args): return subprocess.run([binary,*args],env=env,check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
cli('init','--enable-source','omp','--hosted-processing','no'); cfg=home/'config.toml'; cfg.write_text(cfg.read_text().replace('~/.omp/agent/sessions',str(src)))
daemon=subprocess.Popen([binary,'daemon','run'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
bridge = None
try:
 for _ in range(300):
  if subprocess.run([binary,'status','--json'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0: break
  time.sleep(.1)
 else: raise RuntimeError('daemon unavailable')
 cli('index','--since','2026-10-12','--wait')
 bridge=subprocess.Popen([binary,'mcp'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,bufsize=1)
 def rpc(id,method,params):
  bridge.stdin.write(json.dumps({'jsonrpc':'2.0','id':id,'method':method,'params':params},separators=(',',':'))+'\n'); bridge.stdin.flush()
  while True:
   line=bridge.stdout.readline()
   if not line: raise RuntimeError('bridge closed')
   value=json.loads(line)
   if value.get('id')==id: return value
 rpc(1,'initialize',{'protocolVersion':'2025-11-25','capabilities':{},'clientInfo':{'name':'paired-capture','version':'1'}})
 bridge.stdin.write('{"jsonrpc":"2.0","method":"notifications/initialized"}\n'); bridge.stdin.flush()
 calls=[]; i=1
 for name,(query,answer) in cases.items():
  for target in [1200,512,None]:
   args={'query':query,'mode':'lexical'}
   if target is not None: args['preview_target_bytes']=target
   i+=1; v=rpc(i,'tools/call',{'name':'search_sessions','arguments':args}); calls.append({'case':name,'target':target,'response':v})
   result=json.loads(v['result']['content'][0]['text']); matches=result['results'][0]['matches']; session=result['results'][0]['session_id']
   selection={'kind':'match','match_ref':matches[0]['match_ref']}
   if variant.startswith('candidate'): selection['excerpt_target_bytes']=512
   i+=1; v=rpc(i,'tools/call',{'name':'read_session','arguments':{'session_id':session,'selection':selection,'max_bytes':8192}}); calls.append({'case':name,'target':target,'kind':'read','response':v})
   result=json.loads(v['result']['content'][0]['text']); evidence=''
   while not v['result'].get('isError',False):
    evidence += ''.join(block.get('data','') for item in result.get('items',[]) for block in item.get('blocks',[]))
    cursor=result.get('page',{}).get('next_cursor')
    if not cursor: break
    i+=1; v=rpc(i,'tools/call',{'name':'read_session','arguments':{'cursor':cursor,'max_bytes':8192}}); calls.append({'case':name,'target':target,'kind':'continuation','response':v}); result=json.loads(v['result']['content'][0]['text'])
   if variant.startswith('candidate') and answer not in evidence:
    i+=1; v=rpc(i,'tools/call',{'name':'read_session','arguments':{'session_id':session,'selection':{'kind':'match','match_ref':matches[0]['match_ref']},'max_bytes':8192}}); calls.append({'case':name,'target':target,'kind':'expansion','response':v})
    while not v['result'].get('isError',False):
     result=json.loads(v['result']['content'][0]['text']); cursor=result.get('page',{}).get('next_cursor')
     if not cursor: break
     i+=1; v=rpc(i,'tools/call',{'name':'read_session','arguments':{'cursor':cursor,'max_bytes':8192}}); calls.append({'case':name,'target':target,'kind':'expansion_continuation','response':v})
 db = sqlite3.connect(f'file:{home}/state/index.sqlite?mode=ro', uri=True)
 try:
  for call in calls:
   if 'kind' in call: continue
   result = json.loads(call['response']['result']['content'][0]['text'])
   normalized = []
   excerpts = []
   for session in result['results']:
    matches = []
    session_excerpts = []
    for match in session['matches']:
     encoded = match['match_ref'].split('.')[3]
     lease, ref_id = json.loads(base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)))
     row = db.execute("SELECT payload FROM lease_refs WHERE lease_id=? AND ref_id=? AND kind='match'", (lease, ref_id)).fetchone()
     if row is None: raise RuntimeError('match reference missing from isolated lease database')
     payload = json.loads(row[0])
     matches.append(payload['records'])
     session_excerpts.append(payload.get('excerpt'))
    normalized.append({'session_id': session['session_id'], 'matches': matches})
    excerpts.append(session_excerpts)
   call['normalized'] = normalized
   call['excerpts'] = excerpts
 finally:
  db.close()
 if hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest() != binary_sha256:
  raise RuntimeError('binary changed during capture')
 (root/(variant+'.json')).write_text(json.dumps({'revision':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'binary_sha256':binary_sha256,'sources_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in src.glob('*.jsonl')},'calls':calls},indent=2))
 bridge.terminate(); bridge.wait()
finally:
 if bridge is not None and bridge.poll() is None:
  bridge.terminate(); bridge.wait()
 daemon.terminate(); daemon.wait()
