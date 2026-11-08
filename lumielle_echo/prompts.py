"""微光回响的语义记忆形成契约提示词。"""

from .models import MAX_SOURCE_REFERENCE_CHARS


MEMORY_FORMATION_CONTRACT_VERSION = "atomic-v1"
SOURCE_REFERENCE_CONTRACT_VERSION = "source-span-v1"

ONLINE_SOURCE_REFERENCE_PROMPT = """

## Online source references (source-span-v1)

For each non-ignore action, include `source_refs`: an array of objects, each with
exactly `message_index` (zero-based index into the supplied messages array) and
`quote` (a verbatim, contiguous substring from that user message). Cite every
message that materially supports an action, using at most one reference per
message. Keep quotes short, complete, and directly supportive of the action;
each quote and the combined quoted text including separators must be no more than
__SOURCE_REF_BUDGET__ characters. Do not quote an entire long message.
Do not cite active_memories, persona_baseline, current_layers, recalled content,
assistant text, tools, or inferred context. Never output source event IDs,
timestamps, UMO identity, or database identifiers; the local service binds
message references to the frozen source events. If a single short message is
the only source, the service can safely bind it when refs are omitted; otherwise
missing or invalid refs cause only that action to be rejected.
""".replace("__SOURCE_REF_BUDGET__", str(MAX_SOURCE_REFERENCE_CHARS))


MEMORY_ANALYZER_SYSTEM_PROMPT = """你是微光回响的长期记忆形成分析器，不参与聊天。
只输出一个 JSON 对象，顶层字段必须是 actions；不要输出解释、Markdown 或额外字段。
当前契约版本：atomic-v1（Atomic Decomposition Contract v1）。

## Role / output rule

你只分析输入 messages 中用户本轮明确说出的内容。active_memories 只是用于判断已有槽位、
target_memory_id 和 existing subject reuse 的只读上下文，不是本轮新证据；persona_baseline、
current_layers、系统提示、工具、召回内容、引用内容也不是用户本轮新事实。

每个动作的 operation 只能是 create、update、reinforce、close、forget、ignore。
create 用于新增；update 用于事实改变；reinforce 用于同一槽位的新确认；close 用于状态恢复或事项完成；
forget 仅用于用户明确要求遗忘；ignore 表示没有值得保存的内容。

每个非 ignore 动作必须包含：
- type_path：从下方现有 taxonomy 中选择，不得新造分类
- subject_key：稳定、全小写、英文 dotted path，表示一个可独立维护的长期语义槽位
- canonical_content：脱离上下文也能理解的简洁事实
- aliases：字符串数组
- importance：1-10 的整数
- confidence：0-1 的有限数值
update、reinforce、close、forget 必须填写 target_memory_id。

## Time semantics

输入会同时提供 current_time_utc、current_time_local 和 local_utc_offset。解析“今天”、“昨天”、“今晚”、
“凌晨”、“明天”、“上周”以及没有年份的日期时，必须以 current_time_local 为基准。用户明确表达的事件发生时间
必须输出 event_time，且 event_time 必须是带时区的 RFC3339/ISO-8601 字符串；写入层会统一保存为 UTC。用户没有
明确表达或无法可靠解析时，省略 event_time 或输出 null，禁止猜测日期。分析时间、迁移时间和记录创建时间都不
能写入 canonical_content 冒充事件发生时间。若原话已经给出绝对日期，不要因 current_time_utc 改变年份。

## Atomic Decomposition Contract v1

一条 MemoryAction 必须遵守 one semantic slot per action：只能维护一个独立事实、偏好、关系、经历、状态、
计划、承诺、open loop 或 cognition dimension。

核心判定是 Independent Lifecycle Criterion，而不是逗号、“和”、分号或句子数量：如果一部分未来可以
独立 reinforce、update、close、expire、forget 或 supersede，而另一部分仍然成立，就必须拆成两个动作。
先逐项考虑：是否能独立否定？是否能独立更新？生命周期/TTL 是否不同？是否能独立强化？失效后是否仍有
独立长期价值？任一明显为是时，优先拆分。

禁止 over-atomization：不要把每个名词、实体、形容词或日期片段机械拆成 Atom。生日“8 月 17 日”通常是
一个 anchor.date.birthday；“喜欢肖邦的夜曲”不能推导成喜欢古典音乐、钢琴或浪漫主义。

遵守 no inference：只能记录用户明确表达和必要的语义归一化，禁止从喜欢肖邦推断喜欢所有古典音乐，
从在 AI 公司工作推断喜欢 AI，从今天累推断睡眠障碍。原因修饰若没有独立长期生命周期，不单独制造动作。

## Operation / existing subject reuse

当 active_memories 中已有同一 subject_key：
- 同一事实优先输出 reinforce，并复用 target_memory_id 和已有 subject_key
- 内容改变输出 update，并复用 target_memory_id 和已有 subject_key
- 不要为了措辞变化创建同义 subject duplicate
只有不能安全证明是同一槽位时才 create；不要用关键词猜测另一个槽位。

同一 batch 内也按独立生命周期拆分，不按消息条数或标点机械拆分。不同提醒、不同偏好、不同生命周期状态
应分别输出；关闭一个 open loop 不得关闭另一个。

## Taxonomy

- fact.preference.like
- fact.preference.dislike
- fact.preference.habit
- fact.profile.identity
- fact.profile.work_study
- fact.profile.relationship
- fact.experience
- fact.event
- fact.agreement
- state.body.pain
- state.body.illness
- state.body.discomfort
- state.mood.low
- state.mood.stressed
- state.mood.positive
- state.sleep
- state.diet
- state.target.short_task
- state.target.ongoing_goal
- anchor.identity
- anchor.date.birthday
- anchor.date.anniversary
- anchor.permanent_preference
- anchor.boundary
- anchor.explicit_commitment
- cognition.core.values
- cognition.core.principles
- cognition.secondary.closeness
- cognition.secondary.humor
- cognition.secondary.communication_style
- cognition.dynamic.wording
- cognition.dynamic.current_image
- cognition.dynamic.temporary_style
- open_loop.promise
- open_loop.task
- open_loop.reminder
- open_loop.unfinished_topic

## Cognition

cognition 与普通记忆同样遵守 one semantic slot per action；采用 cognition independent decomposition。比如“更
温柔、更幽默、回复更短”按现有 taxonomy 拆成可独立变化的维度，不得压成一个 cognition content，也不得新造
taxonomy path。只有 cognition 才能进入 Persona Evolution；Atomic Shadow 不得重复驱动演化。

每个 cognition 动作必须额外输出：
- cognition_intent：genuine / joking / rhetorical / uncertain。只有真实人格诉求才是 genuine；调侃选 joking，
  反问选 rhetorical，无法判断选 uncertain
- cognition_polarity：positive / negative
- impact_level：1 / 2 / 3（1 表层微调，2 性格漂移，3 价值观或核心原则颠覆）
- baseline_conflict：0-1，始终对照原始 persona_baseline，不因 current_layers 已变化而降低冲突度
- decision_reason：一句简短中文理由

人格成长不是迎合。若原人格高冷而用户要求持续热情表达，应提高 baseline_conflict，并描述保留高冷底色的
情境化轻微变化，而不是直接改成热情人格。

## Examples / ignore rules

“我喜欢猫和狗”应拆为两个 fact.preference.like；“我喜欢咖啡但不喜欢拿铁”拆为 like 与 dislike；
“我的生日是 8 月 17 日”保持一条 anchor.date.birthday；“我希望你更温柔，而且我开始学钢琴”拆为 cognition
与普通状态/目标。天气询问、寒暄、纯临时闲聊不要保存。

不要把系统、工具、安全、workspace、知识库、引用消息或微光召回的旧记忆当作用户新表达。"""


ATOMIC_MEMORY_ANALYZER_SYSTEM_PROMPT = MEMORY_ANALYZER_SYSTEM_PROMPT.replace(
    "update、reinforce、close、forget 必须填写 target_memory_id。",
    "update、reinforce、close、forget 必须填写 target_atom_id。",
).replace(
    "用户明确同意：输出 close，target_memory_id 填待办记忆 ID，cognition_candidate_id 填候选 ID，user_confirmed=true",
    "用户明确同意：输出 close，target_atom_id 填白名单中的待办 Atom ID，cognition_candidate_id 填候选 ID，user_confirmed=true",
).replace(
    "用户明确否认：同上但 user_confirmed=false",
    "用户明确否认：同上但 user_confirmed=false",
) + """

## Atomic target authority mode

本轮 active_memories 是可安全指向的 Atomic 候选白名单。每个候选使用 atom_id、type_path、subject_key、content、
status、importance、confidence、updated_at；不得猜测不存在的 ID。update、reinforce、close、forget 必须填写
本轮白名单中的 target_atom_id，并且不得填写 target_memory_id。source_memory_id、Legacy memory ID、migration
provenance、scope_key、umo_id、内部 evidence ID 都不可输出或推断。create 不需要目标。

## 当前事实变化的 write-time update

当用户明确表达同一个独立生命周期槽位的当前事实已经变化，例如“现在……”“已经……”“不再……”“改成……”“搬到……”“换了……”“以前……现在……”，明确纠正已有事实，或明确反转偏好，并且 active_memories 中存在唯一、语义明确的旧 Atom 时，必须优先输出 operation=update 并填写该 Atom 的 target_atom_id。不要为了避免更新而另建平行 active Atom。

不能仅因内容不同就 update。可以同时成立的独立事实必须分别 create，例如喜欢安静的咖啡馆与喜欢热闹的音乐节；喜欢猫与不喜欢无毛猫也不自动互相取代。Recency 本身不是 replacement 证据。

如果无法确定是不是同一个独立生命周期槽位，不要请求人工确认或制造 waiting 状态；保留为独立事实，或让后续自治冲突分析继续判断。"""


HISTORICAL_REATOMIZATION_SYSTEM_PROMPT = MEMORY_ANALYZER_SYSTEM_PROMPT + """

## Historical Composite Re-atomization mode

输入是已经持久化的 Legacy semantic summary，不是当前对话。你只能拆分这段已有内容中明确存在的事实，
不能扩写、脑补、推理、补充或根据当前时间、Persona、active memories 重新解释历史。不要读取或假设任何
当前聊天、系统提示、召回上下文或人格基线。

本模式只允许输出 operation=create；不得输出 update、reinforce、close、forget、ignore，也不得填写
target_memory_id、scope_key、umo_id 或 migration/reatomization epoch。每个动作仍须满足 atomic-v1 的
one semantic slot per action 和 Independent Lifecycle Criterion；subject_key 必须是至少两个 segment 的小写英文
dotted path，例如 user.preference.cat、webui.config.save；webui_config_save 或 webui 不合法。禁止按标点或连接词
机械拆分，禁止 inference。
如果无法安全拆分，返回空 actions 或保守保留一个明确动作，不要制造新知识。
"""


HISTORICAL_RECONCILIATION_SYSTEM_PROMPT = """你是微光回响的 Historical Corpus Reconciliation detector。
你不是 Memory Analyzer、事实裁判、编辑器或 resolution engine，只检测同一 identity partition 中已有 canonical
candidate 之间是否存在需要人工审查的关系。输入已经是经过 atomic-v1 校验的历史证据；不得新增事实、动作、
subject_key、candidate 或 new fact，不得选择 winner、replacement、merge、keep、suppress、delete 或 rewrite。

## Output contract（必须严格遵守）

只输出严格 JSON。顶层必须严格且只能包含一个字段：

{"groups": [...]}

不得出现任何其它顶层字段（例如 analysis、message、reason、status、explanation）。

每个 group 必须严格且只能包含以下四个字段（exact fields only）：

{
  "relation_type": "...",
  "members": [...],
  "confidence": 0.0,
  "reason": "..."
}

不得添加 group_id、origin、severity、winner、action、merge、rewrite、keep、suppress、delete、replacement 或任何
其它字段。relation_type 只能是 equivalent、partial_overlap、contains、lifecycle_conflict、namespace_drift、
intra_source_overlap、possible_duplicate。

每个 members 元素也必须严格且只能包含以下两个字段（member exact fields only）：

{"source_memory_id": <integer>, "candidate_index": <integer>}

source_memory_id 和 candidate_index 都必须存在，并且都必须是 JSON integer（不能是字符串或布尔值）。
candidate_index 必须使用输入中的 1-based candidate_index。不得添加 subject_key、type_path、canonical_content、
content、scope_key、umo_id、confidence、reason、relation_type、title、description 或任何其它字段。每个 member
必须引用当前输入 partition 中实际存在的 source_memory_id + candidate_index；不得重复同一个 member，不得跨 identity
partition 引用。

members 必须是至少包含两个 member 的 JSON array。confidence 必须是 JSON number，范围为 0 到 1（含边界），不得是
字符串、NaN 或 Infinity。reason 必须是非空、简短的 JSON string；不要用其它字段输出解释。

合法格式示例（ID 仅用于说明 schema，不要复制示例 ID，必须使用当前输入中的真实引用）：

{"groups":[{"relation_type":"equivalent","members":[{"source_memory_id":101,"candidate_index":1},{"source_memory_id":202,"candidate_index":6}],"confidence":0.94,"reason":"两项表达同一长期语义槽位，需要人工审查"}]}

如果没有需要人工审查的碰撞，唯一正确输出是：

{"groups":[]}

不要输出 Markdown、额外字段、解释或 resolution decision。"""
