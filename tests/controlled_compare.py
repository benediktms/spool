import json,base64,collections,os
A=json.load(open(os.environ['SPOOL_CONTROLLED_EVAL_BASELINE']))
B=json.load(open(os.environ['SPOOL_CONTROLLED_EVAL_DEFAULT_CAPTURE']))
m=json.load(open(os.environ['SPOOL_CONTROLLED_EVAL_MATRIX']))
rel=collections.defaultdict(set)
for x in m['judgments']:
 if x['judgment']=='proposed_relevant': rel[x['query_id']].add(x['session_id'])
def payload(t):
 p=t.split('.')[3]; x=json.loads(base64.urlsafe_b64decode(p+'='*(-len(p)%4)))
 if isinstance(x,list):
  if t.split('.')[1]=='g': return ('g',x[2],x[3])
  if t.split('.')[1]=='i': return ('i',x[2],x[3],x[4],x[5])
  if t.split('.')[1]=='b': return ('b',x[2])
 return tuple((k,x[k]) for k in ('r','d','k','b') if k in x)
def g(t):
 x=payload(t);return (x[1],x[2]) if x[0]=='g' else (dict(x)['r'],dict(x)['d'])
def i(t):
 x=payload(t)
 if x[0]=='i': return (x[1],x[2],x[3],x[4])
 d=dict(x); return(d['k'],d['r'],d.get('b'),d['d'])
def normalize(r):
 out=[]
 for p in r['pages']:
  for it in p['items']:
   k=i(it['item_ref']); meta={n:it.get(n) for n in ('kind','role','phase','timestamp','provenance')}
   if not out or out[-1][0]!=k:out.append((k,meta,[]))
   assert out[-1][1]==meta
   for b in it['blocks']:
    typ=(b['kind'],b['fidelity'],b.get('encoding','utf8'))
    if out[-1][2] and out[-1][2][-1][0]==typ and b.get('start_byte',0)>0:out[-1][2][-1][1].append(b['data'])
    else:out[-1][2].append((typ,[b['data']]))
 return [(k,m,[(t,''.join(v)) for t,v in blocks]) for k,m,blocks in out]
assert len(A)==len(B)
counts=collections.Counter(); groups={}
for a,b in zip(A,B):
 assert (a['mode'],a['query_id'],a['session_id'])==(b['mode'],b['query_id'],b['session_id'])
 key=(a['mode'],a['query_id']); groups.setdefault(key,[]).append((a,b))
 assert len(a['matches'])==len(b['matches'])==len(a['reads'])==len(b['reads'])
 for x,y in zip(a['matches'],b['matches']):
  assert x['preview']==y['preview']; counts['preview_bytes']+=len(y['preview']['text'].encode())
 for x,y in zip(a['reads'],b['reads']):
  assert x['complete'] and y['complete'] and not x.get('error') and not y.get('error')
  assert sorted(map(g,x['pages'][0]['selection']['anchor_message_refs']))==sorted(map(g,y['pages'][0]['selection']['anchor_message_refs']))
  assert normalize(x)==normalize(y)
  counts['reads']+=1
  for z,label in [(x,'old'),(y,'new')]:counts[label+'_pages']+=z['page_count'];counts[label+'_bytes']+=z['serialized_bytes']
def totals(rows):
 o=[]
 for side in (0,1):
  rs=[r for pair in rows for r in pair[side]['reads']];o.append((len(rs),sum(r['page_count'] for r in rs),sum(r['serialized_bytes'] for r in rs)))
 return o
print('ranklists',len(groups),'sessions',len(A),'counts',dict(counts))
lex=[v for k,v in groups.items() if k[0]=='lexical']
print('top',totals([v[0] for v in lex if v]))
print('first_relevant',totals([p for v in lex for p in [next((p for p in v if p[0]['session_id'] in rel[p[0]['query_id']]),None)] if p]))
