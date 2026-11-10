# Changelog

本文件记录面向用户的公开版本变化。

## [0.2.2] - Unreleased

### Fixed

- 遗忘/更正控制屏障按每个动作已经绑定的真实来源判断，避免旧消息与无关新消息混批后重新写回被撤回事实。
- Legacy 检索在异步 Embedding 完成后读取记忆，避免返回等待期间已遗忘、过期或更正的旧快照。
- Atomic 检索不再把零或负相似度当作向量命中；独立词法命中仍可正常召回。
- 人格回滚只撤销目标版本之后的 Atomic Evidence，保留目标之前的有效证据。
- 明确恢复历史人格快照后，可继续形成新的 Dynamic 修订；保留旧候选与旧证据的失效历史。
- Legacy 已生效人格获得同命题新来源时保留支持率、样本和采用门槛；时间线沿真实版本继承/恢复链解释来源。
- 设置页面区分全局默认和用户覆盖，只保存实际修改字段，并可恢复当前用户继承，避免覆盖值意外扩散到其他用户。
- 尊重 AstrBot 明确的无人格选择 `[%None]`，跳过人格基线与成长增量，普通事实记忆仍按现有配置工作。
- 冲突与质量候选排除 `user/bot/assistant` 等共同命名结构，避免无关记忆触发多余模型判断。

### Changed

- 忽略或纯幂等分析不再触发无关的冲突/质量模型维护；失败判断等待下一次实际记忆变更重试，不新增后台重试队列。
- 单条 Atom 补索引改为按指定 ID 查询并复用快照，避免重复扫描其他用户的全部 Atom；人格发现结果未变化时跳过配置写盘。
- 静态阶段 Schema 与离线 Codex CLI 执行器分离，运行安装包和公开源码不再包含该执行器，历史导入 Schema 与私有离线工具保持兼容。
- 总览文案明确新实例默认与已有实例配置的区别；本地运行数据目录整体排除出开发版本控制。
- v0.2.2 为未正式发行修订；正式 Tag / Release 与市场审核分别记录，旧版本 Tag 与附件保持原样。

## [0.2.1]

### Fixed

- 入口、记忆服务、Atomic 写入与记忆质量模块统一使用 `astrbot.api.logger`，移除内置 logging 和入口日志回退，修复 AstrBot Cloud 日志合规问题。
- 离线宿主替身移至 CI/测试脚本，不随运行安装包分发；保留无 Provider 的离线生命周期验证。
- 修复版本递增至 `0.2.1`，同步入口、元数据、NOTICE 和完整更新日志，供新版本重新审核；不覆盖已发布的 `v0.2.0`。

## [0.2.0]

### Added

- Temporal Recall 支持日期范围检索，并提供 `echo_recall`、`echo_recall_range` 只读工具。
- Atomic Lens 支持受限的偏好手动记住、更正和遗忘；明确格式的聊天单条更正/遗忘在当前 UMO 唯一匹配时可完成。
- 已应用的 Dynamic Persona 维度支持基于后续证据形成新修订，并沿用现有门槛、审核、Persona Version 和 Rollback。
- Persona Evolution 使用 Conservative、Balanced（默认）和 Adaptive 自治档位；候选未达门槛时继续观察，用户可以主动提前采用或否决。
- Persona Growth 提供只读当前人格摘要、成长时间线及 Persona Version 变化、冻结触发来源、修订与回滚解释，并支持分页查看来源；后续 Candidate Evidence 不回填旧 Version，同会话覆盖导致的历史缺项明确标为不可恢复。
- 自治 Memory Conflict 对有界候选区分 `replacement`、`coexist`、`deferred`，保留审计历史、运行时未决提示和安全回滚。
- Memory Quality Governance 对有界语义碎片区分 `equivalent`、`related_distinct` 和 `observing`；仅语义等价重复会自动 supersede，并可专用回滚。

### Changed

- Atomic 写入、时间语义、Memory Control、Cognition Runtime authority、人格修订和跨 UMO 操作的边界更加明确；未确认的聊天 Forget/Correction 不再被表述为已完成持久化变更。
- Phase 15 Conflict relation 与 Phase 16 Quality relation 分工：Conflict `coexist` 可继续进入有界 Quality 判断；`replacement`、`deferred` 和仍未解除的 rollback override 不进入等价合并。
- 普通聊天写入后先执行 Conflict reconcile，再执行 Quality reconcile；Quality 每次最多处理 8 组，失败不回滚正常记忆写入。

### Fixed

- 明确聊天更正/遗忘的结果使用自然中文反馈，保留成功与未完成的准确边界，减少内部状态字段和存储术语进入普通回复。
- 开启且可用的 Embedding Provider 下，手动记住、页面更正和明确聊天更正提交后自动补齐新 Atom 的向量索引；复用既有派生索引维护，不阻塞记忆提交；并发写入等待现有索引锁，避免补索引请求被跳过，不扩大查询时的历史回填范围。
- 修复仅选择记忆分区时页面更正/遗忘报错：在同一写事务中建立来源序号屏障，阻止旧排队事件重放，不要求伪造完整聊天身份。
- 修复 AstrBot 插件页沙箱禁止原生确认框导致操作被静默取消：手动记住、更正、遗忘、人格回滚和维护操作改用页内确认，继续校验确认前后的身份快照。
- 手动记忆增改与遗忘后同步刷新画像、身份目录和记忆统计；人格回滚后关闭旧版本详情，避免保留修改前的状态。
- 修复单值日期锚点的 Atomic create/update 歧义，以及明确聊天更正和遗忘对完整当前 UMO 候选集的唯一性检查。
- 阻止明确 Forget 在确定性目标不唯一或持久化失败后再次进入普通后台分析；关闭同轮记忆工具绕过屏障及 raw Cognition 工具泄漏。
- 保持已应用 Persona 内容与待审核 Dynamic 修订互相隔离，并修复冲突/质量 rollback 后同一 Evidence revision 被立即重判的问题。
- Cognition Review 展示实际生效的 Atomic/Legacy Evidence 和来源类型。
- 修复 AstrBot 4.22.0 已有额外消息内容块、却没有临时内容标记接口时的记忆注入报错；该版本使用当轮、不保存的 user context，保留原系统提示和原历史列表，不把召回的用户文本提升为系统指令。
- 运行 ZIP 增加明确顶层包装目录，兼容 AstrBot 4.22.0 管理页上传安装；实际版本加载检查改用 AstrBot 自身的插件解压器。

### Reliability

- 对无数据库且仍使用完整 Schema 兼容默认值的新安装，在首次启动时持久化切换到 Atomic-native / Atomic-only / Atomic Persona authority；已有数据库和自定义 authority 配置不被静默切换。
- 在线消息使用持久 source-event 顺序、冻结的 UMO 身份和有序批次身份；Analyzer 重试/同事件重放不会重复提交权威 Evidence，独立重复表达仍可形成新的强化 Evidence。
- Forget / Correction 成功后写入持久控制 fence，阻止控制之前排队的匹配动作复活旧事实，同时允许控制之后的新表达；Atomic 必需 Legacy mirror 路线保留同事务镜像并按真实事件记录强化 Evidence。
- Scheduler 按完整 UMO 作用域分区；AstrBot 请求首次解析的人格身份绑定到当前事件，后续工具调用不因共享配置回写而漂移。
- 人格候选按具体命题修订绑定 Evidence；用户操作绑定看到的候选快照，旧内容或旧页面响应不能批准新修订。
- 人格独立样本采用 Evidence Episode；Global Persona 回滚保留并验证真实 Atomic 来源身份，不把传输会话或当前用户误当全部来源。
- Atomic 目标查找不再受前 10,000 条历史记录限制；补充来源索引、中文词法、画像批量查询与向量快照复用，异步 Embedding 返回后检查身份内快照是否过期。
- 时间范围上下文以实际注入的完整事件计算分页和续取位置，避免预算截断跳过尚未交付的记录。
- Scheduler 对 buffer、queued 和 inflight 实施真实容量与背压；来源摘要按每个 Atom 对应的消息保存并限制长度，不统一借用批次开头。
- 冲突取代与语义等价整理在本地执行最低置信度约束；低置信结果继续观察，不执行破坏性生命周期变化。
- 历史迁移工具分开统计来源数、候选数、已应用 Atom 数和实际来源覆盖数；个人历史最终化作为独立维护任务，不作为通用插件发行的前置条件。
- 个人 Angel Registry、提示和来源合同改为显式受保护外部 Profile；通用运行代码不携带个人身份资料，Profile 变化后重新校验。
- 运行包采用逐项文件准入与完整依赖检查，退出 14 个不属于运行入口依赖的离线研究和阶段工具；公共源码候选不携带私有 Git 历史、研究快照或真实运行数据。
- 发布目标改为显式参数；拒绝把私有开发仓库和旧发行仓库作为新发行目标，未定名 RC 可以离线验证，但不能通过正式发布检查。
- 私有发布预检与公开质量检查统一核对目标和元数据；复制 RC 源文件复用安全读取，编译验证的缓存放在交付树之外。
- 公共源码 CI 独立执行离线包验证，不需要私有仓库 Token；开发回归与 AstrBot 4.22.0 / 4.25.5 包加载检查分工明确。
- 增加跨渠道连续生命周期、WebUI 实际离线交互、fresh-install 和 Atomic 检索回归。
- 增加 Release Candidate runtime package 的 include/exclude、凭据、metadata/version、WebUI endpoint、运行时工具名检查，以及临时目录解包、插件终止/重启和 Persona lineage 持久性验证。
- AstrBot runtime smoke 使用正式候选包布局导入插件并核对所选版本的 PluginManager metadata loader 与插件版本；目标环境中的页面、Provider、Core 与插件启停仍需实机验收。
- 强化历史记忆来源、身份和时间依据的离线校验；来源不确定时保持未决，不将记录/迁移时间冒充语义事件时间。

### Notes

- `0.2.0` 是本仓库独立发行历史的起始版本；当前未上架 AstrBot 插件市场。
- AstrBot 4.22.0 的兼容验证覆盖核心运行；完整插件 WebUI 使用本轮已验证的 4.25.5，不能用旧版导入成功代替页面验收。
- RC、公开发行与个人历史修复分别登记；离线验证通过不等于真实 Provider、生产数据或公开发行已经验收。完整公开版本日志同步保留在 README。
- Quality 只评估近期、有 lexical overlap 且通过身份/类型边界的候选，不是全库语义扫描；低置信度、单 Evidence 或长期未提及不会单独触发删除或过期。

## [0.1.0] - 2026-09-11

### Added

- Atomic-native 长期记忆写入与 Atomic-only 检索。
- 独立 Evidence、Lifecycle 和 Embedding 派生索引。
- 多 UMO identity 隔离。
- Memory Explorer、Atomic Structure 和 Atomic Lens。
- 确定性 Memory Portrait（记忆画像）。
- LivingMemory Atomic 思想与部分模型/存储基础上的扩展实现。
- Cognition review、Persona Evolution 门禁、Persona versioning、Runtime Persona Overlay 与 rollback。
- Automatic、Semi-automatic、Manual 三种演化模式。

### Notes

First public release.
