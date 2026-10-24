import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const dirs=fs.readdirSync(path.join(root,'chapters')).filter(d=>/^\d{2}-/.test(d)).sort();
const rows=[], failures=[];
const read=f=>fs.readFileSync(path.join(root,f),'utf8');
const write=(f,t)=>fs.writeFileSync(path.join(root,f),t);
const chapters=[], revisions=[], questions=[];
for(const d of dirs){
 const prefix='chapters/'+d+'/';
 for(const f of ['notes.md','revision.md','source-ledger.md'])
  if(!fs.existsSync(path.join(root,prefix+f))) failures.push(prefix+f+' missing');
 const n=read(prefix+'notes.md');
 const nums=[...n.matchAll(/^## (\d+)\./gm)].map(m=>+m[1]);
 if(nums.join(',')!==Array.from({length:22},(_,i)=>i+1).join(',')) failures.push(d+': sections '+nums);
 const study=n.match(/^## 16\.[\s\S]*?(?=^## 17\.)/m)?.[0]||'';
 const qs=[...study.matchAll(/^(?:###\s+)?Q?(\d+)[.)]\s/gm)].map(m=>+m[1]);
 if(qs.join(',')!==Array.from({length:10},(_,i)=>i+1).join(',')) failures.push(d+': questions '+qs);
 if((n.match(/^\s*```/gm)||[]).length%2) failures.push(d+': fences unbalanced');
 if(/\b(TODO|TBD|FIXME)\b/.test(n)) failures.push(d+': unfinished marker');
 const title=n.split(/\r?\n/)[0].replace(/^# /,'');
 const words=n.trim().split(/\s+/).length;
 if(words<1500) failures.push(d+': inspect short chapter');
 rows.push({id:Number(d.slice(0,2)),d,title,words,questions:qs.length,sections:nums.length,diagrams:(n.match(/^```mermaid/gm)||[]).length});
 const relocate=text=>text.replace(/\[([^\]]+)\]\(([^)]+)\)/g,(all,label,url)=>{
  if(/^(https?:|#|\/|[A-Z]:)/i.test(url))return all;
  return '['+label+']('+path.posix.normalize(prefix+url)+')';
 });
 chapters.push('<a id="chapter-'+d.slice(0,2)+'"></a>\n\n'+relocate(n));
 revisions.push('<a id="chapter-'+d.slice(0,2)+'"></a>\n\n'+relocate(read(prefix+'revision.md')));
 questions.push('# '+title+'\n\n'+relocate(study.replace(/^## 16\.[^\n]*\n/,'')));
}
if(rows.map(r=>r.id).join(',')!==Array.from({length:31},(_,i)=>i+1).join(','))failures.push('Expected chapters 1–31');
const total=rows.reduce((n,r)=>n+r.words,0);
const toc=rows.map(r=>'- ['+r.title+'](#chapter-'+r.d.slice(0,2)+')').join('\n');
write('book.md','# Complete technology course\n\n[Course index](README.md) · [Reading guide](start-here.md) · [Glossary](glossary.md)\n\nAll 31 chapters. No dedicated DSA track. Examples are teaching material; runtime execution and learner mastery are separate from written availability.\n\n'+toc+'\n\n---\n\n'+chapters.join('\n\n---\n\n'));
write('revision-book.md','# Complete revision collection\n\n[Course index](README.md) · [Full book](book.md)\n\n'+toc+'\n\n---\n\n'+revisions.join('\n\n---\n\n'));
write('question-bank.md','# Study questions and model answers\n\nTen study questions per chapter. Each full chapter also has a separate unanswered assessment. No learner score is inferred.\n\n'+questions.join('\n\n---\n\n'));
write('README.md','# Complete technology course and notes\n\nLocal project: C:\\Projects\\distributed-commerce. Prepared 2026-10-24.\n\nStart with [the beginner reading guide](start-here.md), then [Chapter 1](chapters/01-foundations/notes.md).\n\n- [Complete book — all 31 chapters](book.md)\n- [All revision notes](revision-book.md)\n- [310 study questions with model answers](question-bank.md)\n- [Beginner glossary](glossary.md)\n- [Lab report template](lab-report-template.md)\n- [Reusable teaching prompt](teaching-prompt.md)\n- [Syllabus coverage](syllabus-coverage.md)\n- [Sources and version policy](source-ledger.md)\n- [Learning and implementation progress](progress.md)\n- [Verification report](verification-report.md)\n\n## Chapter index\n\n| Chapter | Detailed notes | Revision | Words* |\n|---|---|---|---:|\n'+rows.map(r=>'| '+r.id+' | ['+r.title.replace(/^Chapter \d+ [—–-] /,'')+'](chapters/'+r.d+'/notes.md) | [Revision](chapters/'+r.d+'/revision.md) | '+r.words.toLocaleString('en-US')+' |').join('\n')+'\n\n*'+total.toLocaleString('en-US')+' whitespace-counted words across chapter notes, including code/configuration. Length is not proof of mastery.\n\nEach chapter has 22 sections, explanations, diagrams, scoped examples, production/failure reasoning, ten study answers, an exercise with solution, verification guidance, and an unanswered assessment.\n\n## Status\n\nAll chapter materials are prepared. No completed multi-service application, executed infrastructure labs, or production readiness is claimed. Chapter 1 remains Learning; later learner mastery remains Not started until supported by answers and practice. Java 21, Maven, user-approved managed JUnit 6, and no dedicated DSA track remain agreed.\n');
write('source-ledger.md','# Course source ledger\n\nPrepared/checked 2026-10-24. Official documentation, primary specifications/research, and explicitly identified primary engineering/academic references support the notes. Chapter ledgers record versions and access limitations. No unavailable book content is claimed consulted.\n\n## Versions and scope\n\nThe [preserved baseline ledger](baseline-source-ledger.md) records earlier selections. Java 21, proposed Boot 4.1.1, Kafka client 4.2.1/broker 4.2.2, and user-approved managed JUnit 6 remain the teaching baseline, not newly resolved dependency results. Live Spring documentation has advanced. Kafka 4.2 pages now advertise 4.3, and their API banner says 4.2.0. Reverify exact compatibility before implementation.\n\nKafka-core references are Apache-owned. Spring integration, schema formats, and connectors use their respective official owners; these are distinct source categories. Documented facts, project recommendations, illustrative calculations, expected observations, and actual measurements must remain separate. No runtime measurements are invented.\n\n## Chapter ledgers\n\n'+rows.map(r=>'- ['+r.title+'](chapters/'+r.d+'/source-ledger.md)').join('\n')+'\n');
write('progress.md','# Course progress\n\nUpdated 2026-10-24. Project: C:\\Projects\\distributed-commerce.\n\n## Written material\n\nAll 31 chapters have full notes, revision sheets, and source ledgers. Combined book, revision collection, 310 study answers, glossary, reading guide, coverage map, and lab template are available ahead of teaching at the user’s request.\n\n## Learner evidence\n\nChapter 1: Learning. No assessment answer or score recorded. Chapters 2–31: Not started as mastery states. No chapter is marked Practiced or Interview ready merely because its notes exist.\n\n## Implementation evidence\n\nJava 21 was verified previously. Main Maven application and service milestones are not implemented. Maven/Docker/Kafka runtime verification remains pending. Existing Compose is single-node orientation without persistent storage. Embedded examples/tests/manifests/profiling/failure labs remain unexecuted teaching material unless explicitly stated otherwise. No paid cloud resources were created.\n\n## Decisions preserved\n\nNo dedicated DSA. Java 21/Maven. Managed JUnit 6 approved. Proposed dependency baseline retained pending resolution. Order orchestrates Saga; PostgreSQL owns inventory; state and outbox share local transactions; consumers use event and business identities; no cross-service database writes.\n\n## Next study action\n\nRead Chapter 1 and explain the ambiguous Inventory timeout/retry scenario. Record corrections before advancing mastery. Future chapters can be read freely; follow prerequisites and evidence gates for hands-on work. Schedule revision from actual study dates.\n');
write('syllabus-coverage.md','# Syllabus coverage\n\nThe 31 chapters preserve the agreed technology-focused course. The separate DSA track is excluded by user instruction. Angular/TypeScript and optional search remain secondary extensions; no React track is added.\n\n| Area | Chapters | Coverage |\n|---|---|---|\n| Foundations and Java |1–3|Partial failure, modern Java, JVM, concurrency, bounded work|\n| Spring and APIs |4–5|Lifecycle/proxies/transactions; HTTP and integration choices|\n| Relational data |6|JPA, SQL, constraints, isolation, locks, plans, recovery|\n| Kafka core |7–10|Log/storage/KRaft, producer, consumer, ordering and transactions|\n| Kafka applications |11–15|Spring listeners, outbox/inbox/Saga, schemas, Connect/Streams, operations/security/DR|\n| Data and boundaries |16–19|MongoDB, Redis, DDD/CQRS/event sourcing, networking/Gateway|\n| Platform |20–22|Docker, Kubernetes/Helm, AWS/Azure/Terraform, cost/recovery|\n| Production engineering |23–27|Security, observability/SLOs, testing/CI/CD, theory, performance|\n| Design and evidence |28–31|HLD, LLD/patterns, 25 failure labs, interviews/portfolio|\n\n## One project\n\nCustomer, Catalog, Order, Inventory, Payment, Shipping, Notification, Audit/Reporting, and Gateway responsibilities remain within one commerce architecture. Implement progressively: runnable local application, database ownership, Kafka, reliability, projections/caching, deployment, security/observability, then failure/performance evidence.\n\nScoped examples and practice designs are documented. This does not claim every service, module, deployment, or test is implemented. Practice briefs become portfolio evidence only after implementation and review. Reading alone does not establish professional experience or mastery.\n');
const allMarkdown=[];
function walk(dir){for(const e of fs.readdirSync(dir,{withFileTypes:true})){const p=path.join(dir,e.name);if(e.isDirectory())walk(p);else if(e.name.endsWith('.md'))allMarkdown.push(p);}}
walk(root);
let links=0;
for(const file of allMarkdown){
 if(file.endsWith('verification-report.md'))continue;
 const text=fs.readFileSync(file,'utf8');
 for(const m of text.matchAll(/\[[^\]]+\]\(([^)]+)\)/g)){
  let target=m[1];
  if(/^(https?:|mailto:|#)/i.test(target))continue;
  target=target.split('#')[0].replace(/^<|>$/g,'');
  if(!target)continue;
  links++;
  const resolved=path.resolve(path.dirname(file),target);
  // The existing environment guide is delivered beside course/, not staged here.
  if(target==='../windows-environment.md' && fs.existsSync('C:/Projects/distributed-commerce/docs/windows-environment.md'))continue;
  if(!fs.existsSync(resolved))failures.push(path.relative(root,file)+': missing link '+target);
 }
}
const report='# Document verification report\n\nGenerated 2026-10-24 by tools/assemble-course.mjs.\n\n- Chapters: '+rows.length+' /31.\n- Chapter artifacts: '+rows.length*3+' /93 (notes, revision, source ledger).\n- Numbered sections: '+rows.reduce((n,r)=>n+r.sections,0)+' /682.\n- Study questions: '+rows.reduce((n,r)=>n+r.questions,0)+' /310.\n- Mermaid diagrams: '+rows.reduce((n,r)=>n+r.diagrams,0)+'.\n- Notes words including code: '+total.toLocaleString('en-US')+'.\n- Local Markdown links checked: '+links+'.\n- Notes code-fence balance and unfinished markers checked.\n- Technical review corrected consumer position/shutdown and bounded concurrent-test cleanup.\n\n## Findings\n\n'+(failures.length?failures.map(f=>'- '+f).join('\n'):'No structural or local-link failures found.')+'\n\n## Limits\n\nDocument verification does not establish compilation, runtime correctness, rendered Mermaid appearance, infrastructure deployment, or learner mastery. Embedded examples are explicitly unexecuted. Chapter 1 notes and revision were preserved. Destination file integrity is checked separately during delivery.\n';
write('verification-report.md',report);
console.log(JSON.stringify({chapters:rows.length,artifacts:rows.length*3,words:total,questions:rows.reduce((n,r)=>n+r.questions,0),localLinks:links,failures},null,2));
if(failures.length)process.exitCode=1;

