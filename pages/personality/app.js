(async () => {
  const core = window.LumielleEchoPageCore;
  let bridge = null;
  const $ = s => document.querySelector(s);
  let noticeGeneration = 0;
  const notice = (msg, kind = 'info') => {
    const generation = ++noticeGeneration;
    $('#notice').textContent = msg;
    $('#notice').dataset.kind = kind;
    setTimeout(() => {
      if (generation === noticeGeneration) $('#notice').textContent = '';
    }, 4000);
  };
  const text = value => core.escapeHtml(value);
  function confirmAction(message) {
    const dialog = $('#actionConfirmation');
    if (dialog.open) return Promise.resolve(false);
    $('#actionConfirmationMessage').textContent = message;
    dialog.returnValue = 'cancel';
    return new Promise(resolve => {
      dialog.onclose = () => {
        dialog.onclose = null;
        resolve(dialog.returnValue === 'confirm');
      };
      $('#actionConfirmationCancel').onclick = () => dialog.close('cancel');
      $('#actionConfirmationAccept').onclick = () => dialog.close('confirm');
      dialog.showModal();
    });
  }
  const percent = value => `${Math.round(Number(value || 0) * (Number(value) <= 1 ? 100 : 1))}%`;
  // API timestamps remain UTC; presentation is converted in the browser's
  // local timezone through this single formatter.
  const formatTimestamp = value => {
    const raw = String(value || '').trim();
    if (!raw) return '—';
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) return raw;
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium', timeStyle: 'short',
    }).format(parsed);
  };
  const STATUS_LABELS = {
    active: 'active（活跃）', dormant: 'dormant（休眠）', expired: 'expired（已过期）',
    forgotten: 'forgotten（已遗忘）', superseded: 'superseded（已被取代）',
  };
  const PIPELINE_LABELS = { fact: 'Fact（事实）', state: 'State（当前状态）', anchor: 'Anchor（锚点）', cognition: 'Cognition（认知）', open_loop: 'Open Loop（未完成事项）' };
  const MAINTENANCE_WARNING_LABELS = {
    duplicate_active_preference_content: '同内容偏好（运行维护会自动整理）',
  };
  const maintenanceWarningLabel = category =>
    MAINTENANCE_WARNING_LABELS[category] || category || 'warning';
  let memoryView = 'list';
  let memoryItems = [];
  let lastMemoryPayload = null;
  let structurePage = 1;
  let lastStructurePayload = null;
  let lastStructureFilterKey = '';
  let lastMaintenancePreview = null;
  let lastMaintenancePreviewSelectionKey = '';
  let lastMaintenanceHistorySelectionKey = '';
  let maintenanceRequestGeneration = 0;
  let selectionGeneration = 0;
  const selectionRequestGenerations = new Map();
  let selectedIdentityToken = '';
  let selectedPartitionToken = '';
  let growthTimelinePage = 1;
  let openGrowthVersionId = null;
  let growthEvidencePage = 1;
  let currentProfilePayload = null;
  try {
    selectedIdentityToken = sessionStorage.getItem('lumielle_identity_token') || '';
  } catch (e) {
    // sandbox iframe 无 allow-same-origin 时 sessionStorage 抛 SecurityError，降级为内存变量
    selectedIdentityToken = '';
  }

  function ensureBridge() {
    if (!bridge || typeof bridge.apiGet !== 'function' ||
        typeof bridge.apiPost !== 'function') {
      throw new core.PageRequestError(
        'bridge_missing', '未检测到 AstrBot 插件页面 Bridge，请从 AstrBot 插件页面打开本面板'
      );
    }
    return bridge;
  }

  async function get(path, params) {
    return core.requestWithTimeout(() => ensureBridge().apiGet(path, params), 8000);
  }

  async function post(path, body, expectedSelection = null) {
    const requestSelection = expectedSelection ? { ...expectedSelection } : null;
    return core.requestWithTimeout(() => {
      if (requestSelection && !selectionIsCurrent(requestSelection)) {
        throw new core.PageRequestError('selection_stale', '操作对象已变化，请重新加载后重试');
      }
      return ensureBridge().apiPost(path, body);
    }, 8000);
  }

  function waitForBridge(timeoutMs = 8000) {
    const timeout = Math.max(1, Number(timeoutMs) || 8000);
    return new Promise((resolve, reject) => {
      const started = Date.now();
      const timer = setInterval(() => {
        if (window.AstrBotPluginPage &&
            typeof window.AstrBotPluginPage.apiGet === 'function') {
          clearInterval(timer);
          bridge = window.AstrBotPluginPage;
          resolve(bridge);
        } else if (Date.now() - started > timeout) {
          clearInterval(timer);
          reject(new core.PageRequestError(
            'bridge_missing', '等待 AstrBot 插件页面 Bridge 超时，请确认从 AstrBot 插件页面打开本面板'
          ));
        }
      }, 100);
    });
  }

  function renderState(target, state, message, retry = false, selection = selectionSnapshot()) {
    const markup = core.stateMarkup(state, message, retry);
    $(target).innerHTML = retry
      ? markup.replace('data-retry="all"', `data-retry="all" data-selection-generation="${text(selection.generation)}"`)
      : markup;
  }

  function reportError(error, target = null) {
    const message = error?.code === 'timeout'
      ? '读取超时，请检查 AstrBot 状态后重试'
      : String(error?.message || '读取失败，请稍后重试');
    if (target) renderState(target, 'error', message, true);
    $('#apiState').textContent = `异常：${message}`;
    notice(message, 'error');
  }

  function renderReviews(payload, selection = selectionSnapshot()) {
    const items = Array.isArray(payload) ? payload : (payload?.items || []);
    $('#reviews').innerHTML = items.map(item => {
      const evidence = Array.isArray(item.evidence)
        ? item.evidence.map(row => {
          const sourceKind = row?.source_kind;
          const sourceLabel = sourceKind === 'atomic'
            ? 'Atomic（原子证据）'
            : sourceKind === 'legacy'
              ? 'Legacy（旧版证据）'
              : '来源类型未知';
          return `<li>
            <div>${text(row?.content || row?.decision_reason || row)}</div>
            <small>${text(sourceLabel)} · ${text(row?.user_id || '未知用户')} · ${text(row?.session_id || '未知会话')} · ${text(row?.intent || '未知意图')} / ${text(row?.polarity || '未知极性')}</small>
          </li>`;
        }).join('')
        : `<li>${text(item.evidence || '暂无证据说明')}</li>`;
      const candidateStatus = item.status === 'pending'
        ? '观察中'
        : item.status === 'manual_review' || item.status === 'awaiting_confirmation'
          ? '旧状态，等待运行时收敛'
          : text(item.status || '候选');
      return `<article class="card">
        <div class="card-title"><strong>${text(item.content || item.title || '人格候选')}</strong><span class="badge">${candidateStatus}</span></div>
        <ul class="evidence">${evidence}</ul>
        <dl class="metrics">
          <div><dt>支持率</dt><dd>${percent(item.support_rate ?? item.support)}</dd></div>
          <div><dt>所需支持率</dt><dd>${percent(item.required_support)}</dd></div>
          <div><dt>样本 / 所需样本</dt><dd>${text(item.distinct_sessions ?? item.samples ?? 0)} / ${text(item.required_samples ?? 0)}</dd></div>
          <div><dt>冲突度</dt><dd>${percent(item.baseline_conflict ?? item.conflict)}</dd></div>
          <div><dt>影响等级</dt><dd>${text(item.impact_level ?? item.impact ?? 1)} 级</dd></div>
        </dl>
        <div class="actions"><button data-action="approve" data-id="${text(item.id)}" data-revision="${text(item.expected_candidate_revision || '')}" data-selection-generation="${text(selection.generation)}">提前采用</button><button class="deny" data-action="reject" data-id="${text(item.id)}" data-revision="${text(item.expected_candidate_revision || '')}" data-selection-generation="${text(selection.generation)}">否决当前候选</button></div>
      </article>`;
    }).join('') || core.stateMarkup('empty', '当前没有需要查看的人格候选。');
  }

  async function reviewCandidate(action, candidateId, expectedRevision, expectedSelection = selectionSnapshot()) {
    const requestSelection = { ...expectedSelection };
    const revision = String(expectedRevision || '');
    const id = Number(candidateId);
    if (!selectionIsCurrent(requestSelection)) return false;
    if (!['approve', 'reject'].includes(action) || !requestSelection.identityToken) {
      notice('请先选择操作对象并重新加载候选列表。', 'error');
      return false;
    }
    if (!revision || !Number.isInteger(id) || id <= 0) {
      notice('候选快照已失效，请重新加载审核列表。', 'error');
      return false;
    }
    try {
      const result = await post(ENDPOINTS[action], {
        identity_token: requestSelection.identityToken,
        candidate_id: id,
        expected_candidate_revision: revision,
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection)) return false;
      if (result?.ok === false) {
        const failure = new Error(String(result.error || '审核操作失败'));
        failure.code = result.error;
        throw failure;
      }
      return true;
    } catch (error) {
      if (!selectionIsCurrent(requestSelection)) return false;
      const code = [
        error?.code, error?.error, error?.response?.data?.error, error?.message,
      ].filter(Boolean).join(' ');
      if (code.includes('candidate_revision_stale')) {
        notice('候选内容或证据已变化，请重新加载审核列表后再操作。', 'error');
      } else {
        reportError(error);
      }
      return false;
    }
  }

  async function handleReviewAction(target) {
    const requestSelection = selectionFromElement(target);
    if (!selectionIsCurrent(requestSelection)) return false;
    const completed = await reviewCandidate(
      target?.dataset?.action,
      target?.dataset?.id,
      target?.dataset?.revision,
      requestSelection,
    );
    if (completed && selectionIsCurrent(requestSelection)) await load(requestSelection);
    return completed;
  }

  function renderVersions(payload, selection = selectionSnapshot()) {
    const items = Array.isArray(payload) ? payload : (payload?.items || []);
    $('#versions').innerHTML = items.map(item => {
      const dimensions = Object.keys(item.diff || {});
      const rollback = item.status === 'active'
        ? '<span class="badge">当前版本</span>'
        : `<button data-rollback="${text(item.id)}" data-selection-generation="${text(selection.generation)}">回滚</button>`;
      return `<div class="version">
      ${rollback}
      <strong>版本 #${text(item.id)}</strong>
      <div class="meta">${text(formatTimestamp(item.created_at))} · ${text(item.status || '历史版本')} · ${text(item.reason || '')}</div>
      <div class="meta">变更维度：${text(dimensions.join('、') || '无')}</div>
      <button class="button-secondary" type="button" data-version-detail="${text(item.id)}" data-selection-generation="${text(selection.generation)}">查看变化与证据</button>
    </div>`;
    }).join('') || core.stateMarkup('empty', '暂无版本历史。');
  }

  const GROWTH_LAYERS = { core: 'Core', secondary: 'Secondary', dynamic: 'Dynamic' };
  const GROWTH_EVENTS = {
    observation: '观察中', evidence_progress: '证据进展', auto_apply: '自动采用',
    early_adopt: '用户提前采用', apply: '人格版本已应用', reject: '用户否决',
    revision: '后续修订', rollback: 'Persona 回滚', filter: '信号未计入',
    post_apply_signal: '已应用维度的新证据', revision_conflict: '候选保持不变',
    post_reject_signal: '否决冷却期间的新观察',
    post_reject_cooldown_released: '否决冷却结束', timeout: '旧确认记录结束',
    feedback: '用户反馈', other: '其他人格历史事件',
  };
  const GROWTH_CANDIDATE_STATUSES = {
    pending: '观察中', manual_review: '旧状态，待重新评估',
    awaiting_confirmation: '旧状态，待重新评估', approved: '已确认（历史状态）',
    applied: '已采用', rejected: '已否决', filtered: '已过滤',
    passive: '已暂停', invalidated: '已因回滚失效',
  };
  const growthCandidateStatus = value =>
    GROWTH_CANDIDATE_STATUSES[value] || (value ? `记录状态：${value}` : '');

  function renderCurrentPersona(payload) {
    const current = payload?.current_persona || payload || null;
    if (!current) {
      $('#growthCurrentPersona').innerHTML = core.stateMarkup('empty', '选择身份后读取当前已应用的人格版本。');
      return;
    }
    if (current.empty) {
      $('#growthCurrentPersona').innerHTML = `<p class="growth-baseline-note">${text(current.empty_message || '当前仍使用原始 AstrBot Persona，没有已应用的成长增量。')}</p>`;
      return;
    }
    const layers = current.layers || {};
    $('#growthCurrentPersona').innerHTML = ['core', 'secondary', 'dynamic'].map(layer => {
      const entries = Array.isArray(layers[layer]) ? layers[layer] : [];
      const rows = entries.map(item => `<article class="growth-dimension">
        <strong>${text(item.subject || item.subject_key || '未记录主题')}</strong>
        <p>${text(item.content || '历史记录未保存该字段')}</p>
        <small>强度 ${item.strength == null ? '历史记录未保存该字段' : text(percent(item.strength))} · ${item.contextual ? '上下文变化' : '一般变化'} · ${item.status === '观察中' ? '观察中' : text(item.status || '当前生效')}</small>
        <small>来源 Version ${item.source_version_id == null ? '未记录' : `#${text(item.source_version_id)}`} · Candidate ${item.source_candidate_id == null ? '未记录' : `#${text(item.source_candidate_id)}`}${item.observation_candidate_id == null ? '' : ` · 修订观察 Candidate #${text(item.observation_candidate_id)}`}${item.cooldown ? ' · 冷却中' : ''}</small>
      </article>`).join('');
      return `<section class="growth-layer"><h4>${GROWTH_LAYERS[layer]}</h4>${rows || '<p class="muted">当前没有已应用增量。</p>'}</section>`;
    }).join('') + `<small class="growth-baseline-footnote">当前人格增量不会改写原始 AstrBot Persona。</small>`;
  }

  function renderGrowthTimeline(payload, selection = selectionSnapshot()) {
    const events = Array.isArray(payload?.timeline) ? payload.timeline : [];
    $('#growthTimeline').innerHTML = events.map(item => {
      const label = GROWTH_EVENTS[item.event_type] || '其他人格历史事件';
      const layer = item.layer ? GROWTH_LAYERS[item.layer] || item.layer : '';
      const frozenVersion = ['auto_apply', 'early_adopt', 'apply', 'rollback'].includes(item.event_type);
      const evidence = item.evidence_count == null ? '' : `<div class="meta">${frozenVersion ? 'Version 创建时冻结来源条目' : '当前候选有效证据'} ${text(item.evidence_count)}（Atomic ${text(item.atomic_evidence_count || 0)} / Legacy ${text(item.legacy_evidence_count || 0)}）</div>`;
      const metrics = item.support_rate == null ? '' : `<div class="meta">当前候选进度：支持率 ${text(percent(item.support_rate))} / ${text(percent(item.required_support))} · 会话 ${text(item.distinct_sessions || 0)} / ${text(item.required_samples || 0)} · 影响 ${text(item.impact_level ?? '未记录')} · 冲突 ${text(percent(item.baseline_conflict))}</div>`;
      const diff = (item.diff || []).map(change => `<div class="growth-change"><strong>${text(GROWTH_LAYERS[change.layer] || change.layer || '')} · ${text(change.subject_key)}</strong><span>之前：${text(change.before?.content || '历史记录未保存该字段')}</span><span>现在：${text(change.after?.content || '历史记录未保存该字段')}</span></div>`).join('');
      const related = item.related_candidate ? `<div class="growth-related">修订自 Candidate #${text(item.related_candidate.id)}：「${text(item.related_candidate.content)}」</div>` : '';
      const rollback = item.rollback ? `<div class="growth-related">回滚目标 Version #${text(item.rollback.target_version_id)} · 影响 ${text((item.rollback.affected_dimensions || []).join('、') || '历史记录未保存该字段')} · 失效候选 ${text(item.rollback.invalidated_candidate_count)} / Evidence ${text(item.rollback.invalidated_evidence_count)} · 冷却 ${text((item.rollback.cooldown_dimensions || []).join('、') || '无')}</div>` : '';
      const detailButton = item.version_id == null ? '' : `<button class="button-secondary" type="button" data-version-detail="${text(item.version_id)}" data-selection-generation="${text(selection.generation)}">查看版本变化与证据 #${text(item.version_id)}</button>`;
      return `<article class="growth-event growth-event--${text(item.event_type || 'other')}">
        <div class="growth-event-head"><span class="badge">${text(label)}</span><time>${text(formatTimestamp(item.occurred_at))}</time></div>
        <strong>${text(layer ? `${layer} · ` : '')}${text(item.title || label)}</strong>
        <p>${text(item.summary || '历史记录未保存该字段')}</p>
        ${item.candidate_id == null ? '' : `<small>Candidate #${text(item.candidate_id)}${item.status ? ` · ${text(growthCandidateStatus(item.status))}` : ''}${item.subject_key ? ` · ${text(item.subject_key)}` : ''}</small>`}
        ${metrics}${evidence}${item.reason ? `<small>原因：${text(item.reason)}</small>` : ''}${item.technical_event && item.audit_label === '其他人格历史事件' ? `<small>历史事件类型：${text(item.technical_event)}</small>` : ''}
        ${related}${diff}${rollback}${detailButton}
      </article>`;
    }).join('') || core.stateMarkup('empty', '暂无人格成长历史。');
    const page = Number(payload?.pagination?.page || 1);
    const pages = Number(payload?.pagination?.page_count || 1);
    $('#growthTimelinePager').innerHTML = `<button type="button" data-growth-page="prev" data-selection-generation="${text(selection.generation)}" ${page <= 1 ? 'disabled' : ''}>上一页</button><span>第 ${text(page)} / ${text(pages)} 页 · ${text(payload?.pagination?.total || 0)} 条</span><button type="button" data-growth-page="next" data-selection-generation="${text(selection.generation)}" ${page >= pages ? 'disabled' : ''}>下一页</button>`;
  }

  function renderVersionDetail(payload, selection = selectionSnapshot()) {
    const root = $('#personaVersionDetail');
    const detail = payload && payload.version ? payload : null;
    if (!detail) {
      root.hidden = true;
      root.innerHTML = '';
      return;
    }
    const version = detail.version;
    const changes = (detail.changes || []).map(change => `<article class="growth-change">
      <strong>${text(GROWTH_LAYERS[change.layer] || change.layer || '')} · ${text(change.subject_key)}</strong>
      <span>之前：${text(change.before?.content || '历史记录未保存该字段')}</span>
      <span>现在：${text(change.after?.content || '历史记录未保存该字段')}</span>
      <small>强度：${change.before?.strength == null ? '未记录' : text(percent(change.before.strength))} → ${change.after?.strength == null ? '未记录' : text(percent(change.after.strength))}</small>
    </article>`).join('') || `<p class="muted">${text(detail.changes_notice || '历史记录未保存详细变更')}</p>`;
    const lineage = detail.revision_lineage;
    const lineageHtml = lineage?.candidate ? `<div class="growth-related"><strong>修订链</strong><br>${lineage.revised_from ? `Candidate #${text(lineage.revised_from.id)}「${text(lineage.revised_from.content)}」 → ` : ''}Candidate #${text(lineage.candidate.id)}「${text(lineage.candidate.content)}」${(lineage.revisions || []).map(item => ` → Candidate #${text(item.id)}「${text(item.content)}」`).join('')}</div>` : '';
    const evidence = detail.evidence || {};
    const evidenceHtml = (evidence.items || []).map(item => `<li>
      <div>${text(item.content || '来源记忆不可用')}</div>
      <small>${text(item.source_kind === 'atomic' ? 'Atomic（原子证据）' : item.source_kind === 'legacy' ? 'Legacy（旧版证据）' : '来源类型未知')} · ${text(item.user_id || '用户未记录')} / ${text(item.session_id || '会话未记录')} · ${text(item.intent || '意图未记录')} / ${text(item.polarity || '极性未记录')} · 影响 ${text(item.impact_level ?? '未记录')} · 冲突 ${text(percent(item.baseline_conflict))}${item.invalidated ? ' · 已因回滚失效' : ''}</small>
      ${item.source_unavailable ? '<small>来源记录不可用，未使用其他来源替代。</small>' : ''}
      ${item.atom_id == null ? '' : `<button type="button" class="button-secondary" data-growth-atom="${text(item.atom_id)}" data-selection-generation="${text(selection.generation)}">在 Atomic Lens 查看 #${text(item.atom_id)}</button>`}
    </li>`).join('') || '<li class="muted">暂无可展示证据。</li>';
    const evidencePage = Number(evidence.page || 1);
    const evidencePages = Number(evidence.page_count || 1);
    root.innerHTML = `<div class="growth-detail-head"><div><h3>Persona Version #${text(version.id)}</h3><small>${text(formatTimestamp(version.created_at))} · ${text(version.status)} · Parent ${version.parent_version_id == null ? '未记录' : `#${text(version.parent_version_id)}`}${version.rollback_of_version_id == null ? '' : ` · 回滚目标 #${text(version.rollback_of_version_id)}`}</small></div><button type="button" class="button-secondary" data-growth-detail-close data-selection-generation="${text(selection.generation)}">关闭</button></div>
      <p><strong>原因：</strong>${text(detail.why?.reason || '历史记录未保存该字段')} · <strong>来源：</strong>${text(detail.why?.decision_source || '未记录')} · <strong>影响等级：</strong>${text(detail.why?.impact_level ?? '未记录')}</p>
      <h4>发生了什么</h4><div class="growth-change-list">${changes}</div>${lineageHtml}
      ${detail.rollback ? `<div class="growth-related">回滚解释：失效候选 ${text(detail.rollback.invalidated_candidate_count)} 条，Evidence ${text(detail.rollback.invalidated_evidence_count)} 条；冷却维度 ${(detail.rollback.cooldown_dimensions || []).map(text).join('、') || '无'}。</div>` : ''}
      <h4>Version 创建时冻结来源</h4><p class="muted">此处只展示版本自身保存的来源谱系；候选后续观察进度不会回填到旧版本。</p>${evidence.notice ? `<p class="muted">${text(evidence.notice)}</p>` : ''}<ul class="evidence growth-evidence">${evidenceHtml}</ul>
      <div class="growth-pager"><button type="button" data-version-evidence-page="prev" data-selection-generation="${text(selection.generation)}" ${evidencePage <= 1 ? 'disabled' : ''}>上一页证据</button><span>第 ${text(evidencePage)} / ${text(evidencePages)} 页 · ${text(evidence.total || 0)} 条</span><button type="button" data-version-evidence-page="next" data-selection-generation="${text(selection.generation)}" ${evidencePage >= evidencePages ? 'disabled' : ''}>下一页证据</button></div>`;
    root.hidden = false;
  }

  function renderMemoryList(payload, selection = selectionSnapshot()) {
    const items = Array.isArray(payload?.items) ? payload.items : [];
    lastMemoryPayload = {
      items,
      page: Number(payload?.page || 1),
      page_size: Number(payload?.page_size || 24),
      total: Number(payload?.total || 0),
    };
    memoryItems = lastMemoryPayload.items;
    const statusLabel = item => text(item.status_label || STATUS_LABELS[item.status] || item.status || 'unknown（未知）');
    const list = items.map(item => `<button class="memory-row" type="button" data-memory-id="${text(item.id)}" data-atom-lens="${text(item.id)}" data-selection-generation="${text(selection.generation)}">
      <span class="memory-row-main"><strong>${text(item.summary_title || item.content || '未命名记忆')}</strong><small>${text(item.type_label || item.type_path)} · ${text(item.subject_key || '无主题')}</small></span>
      <span class="memory-row-meta"><b>${statusLabel(item)}</b><small>${text(formatTimestamp(item.updated_at || item.created_at))}</small></span>
    </button>`).join('');
    $('#memoryList').innerHTML = list || core.stateMarkup('empty', '当前筛选条件下无原子记忆。');
    syncMemoryViews();
    const page = lastMemoryPayload.page;
    const pageSize = lastMemoryPayload.page_size;
    const total = lastMemoryPayload.total;
    const pages = Math.max(1, Math.ceil(total / pageSize));
    $('#memoryPager').innerHTML = `<button type="button" data-memory-page="prev" data-selection-generation="${text(selection.generation)}" ${page <= 1 ? 'disabled' : ''}>上一页</button><span>第 ${page} / ${pages} 页 · ${total} 条</span><button type="button" data-memory-page="next" data-selection-generation="${text(selection.generation)}" ${page >= pages ? 'disabled' : ''}>下一页</button>`;
  }

  function syncMemoryViews() {
    const showList = memoryView === 'list';
    const showAtomic = memoryView === 'atomic';
    const showMaintenance = memoryView === 'maintenance';
    const listPanel = $('#memoryListView');
    const structurePanel = $('#atomicStructureView');
    const maintenancePanel = $('#memoryMaintenanceView');
    if (listPanel) {
      listPanel.hidden = !showList;
      listPanel.classList.toggle('is-active', showList);
      listPanel.setAttribute('aria-hidden', showList ? 'false' : 'true');
    }
    if (structurePanel) {
      structurePanel.hidden = !showAtomic;
      structurePanel.classList.toggle('is-active', showAtomic);
      structurePanel.setAttribute('aria-hidden', showAtomic ? 'false' : 'true');
    }
    if (maintenancePanel) {
      maintenancePanel.hidden = !showMaintenance;
      maintenancePanel.classList.toggle('is-active', showMaintenance);
      maintenancePanel.setAttribute('aria-hidden', showMaintenance ? 'false' : 'true');
    }
  }

  function syncRememberForm() {
    const button = $('#memoryRememberSubmit');
    const content = $('#memoryRememberContent');
    if (!button) return;
    button.disabled = !((selectedIdentityToken || selectedPartitionToken) && String(content?.value || '').trim());
  }

  function clearRememberForm() {
    const content = $('#memoryRememberContent');
    if (content) content.value = '';
    syncRememberForm();
  }

  function setSelection(identityToken = '', partitionToken = '') {
    const nextIdentity = String(identityToken || '');
    const nextPartition = String(partitionToken || '');
    if (nextIdentity !== selectedIdentityToken || nextPartition !== selectedPartitionToken) {
      selectionGeneration += 1;
      maintenanceRequestGeneration += 1;
      growthTimelinePage = 1;
      openGrowthVersionId = null;
      growthEvidencePage = 1;
      memoryPage = 1;
      structurePage = 1;
      lastMemoryPayload = null;
      memoryItems = [];
      lastStructurePayload = null;
      lastStructureFilterKey = '';
      currentProfilePayload = null;
      selectedIdentityToken = nextIdentity;
      selectedPartitionToken = nextPartition;
      $('#identity').value = selectedIdentityToken;
      clearSelectionSensitiveViews();
      clearRememberForm();
      clearMaintenancePreview('操作对象已变化，请重新生成 Preview。');
      clearMaintenanceHistory();
      noticeGeneration += 1;
      $('#notice').textContent = '';
      $('#apiState').textContent = '';
      const partitionOnly = !selectedIdentityToken && Boolean(selectedPartitionToken);
      $('#scopeContext').textContent = partitionOnly ? '当前分支：仅记忆分区' : '当前分支：正在切换身份';
      $('#identityState').textContent = partitionOnly ? '未关联会话身份' : '正在切换身份';
      $('#identitySummary').textContent = partitionOnly ? '仅 Atomic 记忆分区' : '正在切换身份';
      renderState('#personaSummary', partitionOnly ? 'empty' : 'loading',
        partitionOnly ? '当前记忆分区未关联可操作的人格身份。' : '正在读取当前身份人格……');
      return;
    }
    selectedIdentityToken = nextIdentity;
    selectedPartitionToken = nextPartition;
  }

  function selectionSnapshot() {
    return {
      identityToken: selectedIdentityToken,
      partitionToken: selectedPartitionToken,
      generation: selectionGeneration,
    };
  }

  function selectionIsCurrent(snapshot) {
    return Boolean(snapshot) && snapshot.generation === selectionGeneration &&
      snapshot.identityToken === selectedIdentityToken &&
      snapshot.partitionToken === selectedPartitionToken;
  }

  function beginSelectionRequest(name, expectedSelection = selectionSnapshot()) {
    if (!selectionIsCurrent(expectedSelection)) return null;
    const sequence = (selectionRequestGenerations.get(name) || 0) + 1;
    selectionRequestGenerations.set(name, sequence);
    return { ...expectedSelection, requestName: name, requestSequence: sequence };
  }

  function selectionRequestIsCurrent(request) {
    return Boolean(request) && selectionIsCurrent(request) &&
      selectionRequestGenerations.get(request.requestName) === request.requestSequence;
  }

  function invalidateSelectionRequests(prefix) {
    for (const [name, sequence] of selectionRequestGenerations) {
      if (name.startsWith(prefix)) selectionRequestGenerations.set(name, sequence + 1);
    }
  }

  function selectionFromElement(element, fallback = selectionSnapshot()) {
    const value = element?.dataset?.selectionGeneration;
    if (value === undefined || value === '') return { ...fallback };
    return { ...fallback, generation: Number(value) };
  }

  function selectionParameters(selection, extra = {}) {
    return {
      identity_token: selection.identityToken,
      partition_token: selection.partitionToken,
      ...extra,
    };
  }

  function clearSelectionSensitiveViews() {
    renderState('#reviews', 'loading', '正在读取认知候选……');
    renderState('#versions', 'loading', '正在读取版本历史……');
    renderCurrentPersona(null);
    renderState('#growthTimeline', 'loading', '正在读取当前身份的人格成长记录……');
    $('#growthTimelinePager').innerHTML = '';
    renderVersionDetail(null);
    renderState('#profileContent', 'loading', '正在生成当前身份画像……');
    renderState('#memoryOverview', 'loading', '正在读取当前身份概览……');
    renderState('#memoryCompositionStrip', 'loading', '正在读取当前身份记忆构成……');
    renderState('#memoryList', 'loading', '正在读取 Atomic 记忆……');
    $('#memoryPager').innerHTML = '';
    $('#atomicStructureSummary').innerHTML = '';
    renderState('#atomicStructureList', 'loading', '正在读取 Atomic 结构……');
    $('#atomicStructurePager').innerHTML = '';
    const detail = $('#memoryDetail');
    detail.hidden = true;
    detail.classList.remove('is-open');
    detail.innerHTML = '';
    renderPortraitDirectory([]);
  }

  function structureNodeMarkup(item, selection = selectionSnapshot()) {
    return `<button type="button" class="structure-node" data-atomic-node="${text(item.id)}" data-selection-generation="${text(selection.generation)}" aria-label="ATOM #${text(item.id)} ${text(item.content || '')}">
      <span class="structure-node-id">ATOM #${text(item.id)}</span>
      <strong>${text(item.summary || item.content || '未命名原子')}</strong>
      <small>${text(item.type_label || item.type_path)} · ${text(item.status_label || item.status)}</small>
      <em>Evidence（证据） ×${text(item.evidence_count ?? 0)} · importance（重要度） ${percent(item.importance)} · confidence（置信度） ${percent(item.confidence)} · ${item.embedding_state === 'indexed' ? 'Embedding（向量索引）已建立' : 'Embedding（向量索引）待建立'} · TTL（有效期） ${text(item.expires_at ? formatTimestamp(item.expires_at) : '长期')}</em>
    </button>`;
  }

  function renderAtomicStructure(payload, selection = selectionSnapshot()) {
    lastStructurePayload = payload || { subjects: [], summary: {} };
    structurePage = Math.max(1, Number(lastStructurePayload.page || 1));
    const summary = lastStructurePayload.summary || {};
    const subjects = Array.isArray(lastStructurePayload.subjects) ? lastStructurePayload.subjects : [];
    const summaryRoot = $('#atomicStructureSummary');
    const listRoot = $('#atomicStructureList');
    if (!summaryRoot || !listRoot) return;
    summaryRoot.innerHTML = [
      ['Subjects（主题）', summary.subjects ?? 0],
      ['Atoms（原子）', summary.atoms ?? 0],
      ['Multi-atom Subjects（多原子主题）', summary.multi_atom_subjects ?? 0],
      ['Singleton Subjects（单原子主题）', summary.singleton_subjects ?? 0],
      ['Evidence（证据）', summary.evidence ?? 0],
    ].map(([label, value]) => `<div class="structure-summary-card"><span>${text(label)}</span><strong>${text(value)}</strong></div>`).join('');
    if (!Number(summary.atoms || 0)) {
      listRoot.innerHTML = core.stateMarkup('empty', '当前筛选条件下无原子记忆。');
    } else {
      const singletonNotice = Number(summary.multi_atom_subjects || 0)
        ? ''
        : '<div class="structure-singleton-notice">当前结果均为单原子主题。</div>';
      listRoot.innerHTML = singletonNotice + subjects.map(subject => {
        const preview = Array.isArray(subject.atom_preview) ? subject.atom_preview : [];
        const stats = `${text(subject.atom_count)} Atom（原子） · ${text(subject.evidence_count)} Evidence（证据） · active（活跃） ${text(subject.active_count)} · superseded（已被取代） ${text(subject.superseded_count)}`;
        if (Number(subject.atom_count) === 1) {
          return `<section class="structure-singleton" data-structure-subject="${text(subject.subject_key)}">
            <button type="button" class="structure-singleton-node" data-atomic-node="${text(preview[0]?.id || '')}" data-selection-generation="${text(selection.generation)}">
              <span class="structure-singleton-dot" aria-hidden="true">○</span><span><strong>Singleton（单原子主题）</strong><b>${text(subject.subject_key || '未命名主题')}</b><small>${stats}</small></span>
            </button>
            <button type="button" class="structure-list-link" data-structure-subject-list="${text(subject.subject_key)}" data-selection-generation="${text(selection.generation)}">在列表查看</button>
          </section>`;
        }
        return `<section class="structure-cluster" data-structure-subject="${text(subject.subject_key)}">
          <div class="structure-subject"><span>Subject（主题）</span><strong>${text(subject.subject_key || '未命名主题')}</strong><small>${stats}</small></div>
          <div class="structure-node-grid">${preview.map(item => structureNodeMarkup(item, selection)).join('')}</div>
          <button type="button" class="structure-list-link" data-structure-subject-list="${text(subject.subject_key)}" data-selection-generation="${text(selection.generation)}">在列表查看全部 Atom</button>
        </section>`;
      }).join('');
    }
    const page = Number(lastStructurePayload.page || 1);
    const pageSize = Number(lastStructurePayload.page_size || 20);
    const total = Number(lastStructurePayload.total_subjects || 0);
    const pages = Math.max(1, Math.ceil(total / pageSize));
    $('#atomicStructurePager').innerHTML = `<button type="button" data-structure-page="prev" data-selection-generation="${text(selection.generation)}" ${page <= 1 ? 'disabled' : ''}>上一页</button><span>第 ${page} / ${pages} 页 · ${total} 个主题</span><button type="button" data-structure-page="next" data-selection-generation="${text(selection.generation)}" ${page >= pages ? 'disabled' : ''}>下一页</button>`;
    syncMemoryViews();
  }

  function syncMaintenanceApplyButton() {
    const applyButton = $('#memoryMaintenanceApply');
    if (!applyButton) return;
    applyButton.disabled = !lastMaintenancePreview;
    if (lastMaintenancePreview && (
      lastMaintenancePreviewSelectionKey !== maintenanceSelectionKey() ||
      !lastMaintenancePreview.plan_token ||
      !Number(lastMaintenancePreview.applyable_count || 0)
    )) applyButton.disabled = true;
  }

  function clearMaintenancePreview(message = '尚未生成维护 Preview。') {
    maintenanceRequestGeneration += 1;
    lastMaintenancePreview = null;
    lastMaintenancePreviewSelectionKey = '';
    const preview = $('#memoryMaintenancePreview');
    const actions = $('#memoryMaintenanceActions');
    const warnings = $('#memoryMaintenanceWarnings');
    if (preview) preview.innerHTML = `<p class="muted">${text(message)}</p>`;
    if (actions) actions.innerHTML = '';
    if (warnings) warnings.innerHTML = '';
    if ($('#memoryMaintenanceActionCount')) $('#memoryMaintenanceActionCount').textContent = '0';
    if ($('#memoryMaintenanceWarningCount')) $('#memoryMaintenanceWarningCount').textContent = '0';
    syncMaintenanceApplyButton();
  }

  function clearMaintenanceHistory(message = '选择身份后读取当前 UMO 的整理历史。') {
    lastMaintenanceHistorySelectionKey = '';
    const target = $('#memoryMaintenanceHistory');
    if (target) target.innerHTML = `<p class="muted">${text(message)}</p>`;
    const summary = $('#memoryHealthSummary');
    if (summary) summary.innerHTML = `<p class="muted">${text(message)}</p>`;
  }

  function renderMaintenanceHistory(payload, selectionKey = maintenanceSelectionKey(), selection = selectionSnapshot()) {
    const items = Array.isArray(payload?.consolidations) ? payload.consolidations : [];
    lastMaintenanceHistorySelectionKey = selectionKey;
    const consolidations = items.map(item => {
      const members = Array.isArray(item.member_atom_ids) ? item.member_atom_ids : [];
      const redundant = Array.isArray(item.redundant_atom_ids) ? item.redundant_atom_ids : [];
      const rolledBack = Boolean(item.rolled_back_at);
      const action = rolledBack
        ? `<span class="badge">已回滚</span>`
        : `<button type="button" class="button-secondary" data-consolidation-rollback="${text(item.id)}">回滚本次整理</button>`;
      return `<article class="maintenance-row" data-consolidation-operation="${text(item.id)}">
        <div><strong>操作 #${text(item.id)} · ${text(item.type_path || '')}</strong><span>保留 canonical ATOM #${text(item.canonical_atom_id)}；整理时 superseded ${text(redundant.map(id => `#${id}`).join('、') || '—')}</span><small>分组成员 ${text(members.map(id => `#${id}`).join('、'))} · ${text(formatTimestamp(item.created_at))} · ${text(item.reason_code || '')}${rolledBack ? ' · 同一分组暂不自动重整' : ''}</small></div>
        <div class="maintenance-row-meta">${action}</div>
      </article>`;
    });
    const ttlEvents = Array.isArray(payload?.ttl_events) ? payload.ttl_events : [];
    const ttlMarkup = ttlEvents.map(item => `<article class="maintenance-row"><div><strong>ATOM #${text(item.atom_id)} · ${text(item.type_path || '')}</strong><span>${text(item.summary || '')}</span><small>${text(formatTimestamp(item.created_at))} · ${text(item.reason_code || '')}</small></div><div class="maintenance-row-meta">active → expired</div></article>`);
    const conflicts = Array.isArray(payload?.conflict_resolutions) ? payload.conflict_resolutions : [];
    const conflictMarkup = conflicts.map(item => {
      const rolledBack = Boolean(item.rolled_back_at);
      const relation = rolledBack ? '已回滚' : ({
        replacement: '自动替代', coexist: '可共存', deferred: '继续观察',
      }[item.relation] || '关系未知');
      const currentId = Number(item.current_atom_id || 0);
      const superseded = Array.isArray(item.superseded_atom_ids) ? item.superseded_atom_ids : [];
      const members = Array.isArray(item.members) ? item.members : [];
      const memberMarkup = members.map(member => {
        const atomId = Number(member.atom_id || 0);
        const prefix = atomId === currentId ? '当前候选' : superseded.includes(atomId) ? '旧候选' : '成员';
        return `<button type="button" class="maintenance-atom-review" data-conflict-atom="${text(atomId)}">${text(prefix)} · ATOM #${text(atomId)} · ${text(member.content || '内容不可用')}</button>`;
      }).join('');
      const transition = item.relation === 'replacement'
        ? `<span>${text(superseded.map(id => `#${id}`).join('、') || '—')} → #${text(currentId || '—')}</span>`
        : `<span>${text(members.map(member => `#${member.atom_id} ${member.content || '内容不可用'}`).join('；') || '—')}</span>`;
      const action = item.relation === 'replacement' && !rolledBack
        ? `<button type="button" class="button-secondary" data-conflict-rollback="${text(item.id)}">回滚冲突判断</button>`
        : rolledBack ? '<span class="badge">已回滚</span>' : '<span class="badge">无生命周期变更</span>';
      const source = item.decision_source === 'write_time_update' ? '在线明确更新' : '语义冲突分析';
      return `<article class="maintenance-row" data-conflict-resolution="${text(item.id)}">
        <div><strong>冲突 #${text(item.id)} · ${text(relation)}</strong>${transition}<div class="maintenance-related-review">${memberMarkup}</div><small>${text(source)} · ${text(formatTimestamp(item.created_at))} · ${text(item.reason || '')}${rolledBack ? ` · 回滚于 ${text(formatTimestamp(item.rolled_back_at))}` : ''}</small></div>
        <div class="maintenance-row-meta">${action}</div>
      </article>`;
    });
    const qualityResolutions = Array.isArray(payload?.quality_resolutions) ? payload.quality_resolutions : [];
    const qualityLabels = {
      equivalent: '语义等价',
      related_distinct: '相关但独立',
      observing: '证据不足，继续观察',
    };
    const qualityMarkup = qualityResolutions.map(item => {
      const relation = String(item.relation || '');
      const members = Array.isArray(item.members) ? item.members : [];
      const memberIds = Array.isArray(item.member_atom_ids) ? item.member_atom_ids : [];
      const redundantIds = Array.isArray(item.redundant_atom_ids) ? item.redundant_atom_ids : [];
      const rolledBack = Boolean(item.rolled_back_at);
      const isAppliedEquivalent = relation === 'equivalent' &&
        item.reason_code === 'auto_semantic_equivalent_consolidation';
      const action = isAppliedEquivalent
        ? rolledBack
          ? '<span class="badge">已回滚</span>'
          : `<button type="button" class="button-secondary" data-quality-rollback="${text(item.id)}">回滚语义整理</button>`
        : '<span class="badge">未修改</span>';
      const memberMarkup = members.map(member => {
        const atomId = Number(member.atom_id || 0);
        const prefix = atomId === Number(item.canonical_atom_id || 0)
          ? 'canonical' : redundantIds.includes(atomId) ? '待整理' : '成员';
        return `<button type="button" class="maintenance-atom-review" data-conflict-atom="${text(atomId)}">${text(prefix)} · ATOM #${text(atomId)} · ${text(member.content || '内容不可用')}</button>`;
      }).join('');
      const transition = isAppliedEquivalent
        ? `保留 canonical ATOM #${text(item.canonical_atom_id || '—')}；整理 ${text(redundantIds.map(id => `#${id}`).join('、') || '—')}`
        : '未改变 Atom 生命周期';
      return `<article class="maintenance-row" data-quality-resolution="${text(item.id)}">
        <div><strong>质量判断 #${text(item.id)} · ${text(qualityLabels[relation] || '关系未知')}</strong><span>${text(transition)}</span><div class="maintenance-related-review">${memberMarkup}</div><small>${text(formatTimestamp(item.created_at))} · ${text(item.reason || '')}${rolledBack ? ` · 回滚于 ${text(formatTimestamp(item.rolled_back_at))}` : ''}</small></div>
        <div class="maintenance-row-meta">${action}</div>
      </article>`;
    });
    const conflictLabels = [
      ['自动 Exact Consolidation', 'exact_consolidation'], ['TTL Expiry', 'ttl_expiry'],
      ['Conflict Replacement', 'replacement'], ['Coexist', 'coexist'],
      ['Deferred Conflict', 'deferred'], ['Conflict Rollback', 'rolled_back'],
      ['Semantic Equivalent Consolidation', 'semantic_equivalent'],
      ['Related Distinct', 'related_distinct'], ['Quality Observing', 'quality_observing'],
      ['Quality Rollback', 'quality_rollback'],
      ['Unsupported Active', 'unsupported_active'],
      ['Hard-expiry Integrity Anomaly', 'hard_expiry_integrity_anomaly'],
      ['User Rollback', 'user_rollback'],
    ];
    const health = payload?.health_summary || {};
    $('#memoryHealthSummary').innerHTML = `<div class="maintenance-summary">${conflictLabels.map(([label, key]) => `<div class="maintenance-summary-card"><span>${text(label)}</span><strong>${text(health[key] ?? 0)}</strong></div>`).join('')}</div>`;
    const history = conflictMarkup.concat(qualityMarkup, consolidations, ttlMarkup).join('') || core.stateMarkup('empty', '当前身份暂无自治维护、冲突与质量历史。');
    $('#memoryMaintenanceHistory').innerHTML = history.replace(/<button\b(?![^>]*\bdata-selection-generation=)/g,
      `<button data-selection-generation="${text(selection.generation)}"`);
  }

  async function loadMemoryMaintenanceHistory() {
    if (!selectedIdentityToken && !selectedPartitionToken) {
      clearMaintenanceHistory();
      return;
    }
    const selection = maintenanceSelection();
    const requestKey = maintenanceSelectionKey(selection);
    const requestGeneration = maintenanceRequestGeneration;
    const request = beginSelectionRequest('maintenance-history');
    if (!request) return;
    try {
      const payload = await get(ENDPOINTS.memoryMaintenanceHistory, selection);
      if (!selectionRequestIsCurrent(request) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      renderMaintenanceHistory(payload, requestKey, request);
    } catch (error) {
      if (!selectionRequestIsCurrent(request) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      reportError(error, '#memoryMaintenanceHistory');
    }
  }

  async function rollbackMemoryConsolidation(operationId) {
    const requestKey = lastMaintenanceHistorySelectionKey;
    const requestGeneration = maintenanceRequestGeneration;
    const requestSelection = selectionSnapshot();
    if (!requestKey || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
    const selection = maintenanceSelection();
    try {
      await post(ENDPOINTS.memoryMaintenanceRollback, {
        ...selection, operation_id: Number(operationId),
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      await loadMemoryMaintenanceHistory();
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      notice('本次自动整理已回滚；相同分组会暂时保留。');
      loadMemoryPage(memoryPage, requestSelection).catch(error => reportError(error, '#memoryList'));
    } catch (error) {
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      reportError(error, '#memoryMaintenanceHistory');
    }
  }

  async function rollbackMemoryConflict(resolutionId) {
    const requestKey = lastMaintenanceHistorySelectionKey;
    const requestGeneration = maintenanceRequestGeneration;
    const requestSelection = selectionSnapshot();
    const operation = Number(resolutionId);
    if (!Number.isInteger(operation) || operation <= 0 || !requestKey ||
        !selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
    const selection = maintenanceSelection();
    if (!(await confirmAction('撤销这次自动 replacement 判断？被取代的 Atom 将恢复为 active，当前新 Atom 保留，不删除任何 Evidence。'))) return;
    if (!maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
    try {
      await post(ENDPOINTS.memoryConflictRollback, {
        ...selection, resolution_id: operation,
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      await loadMemoryMaintenanceHistory();
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      notice('冲突 replacement 判断已回滚；所有 Atom 与 Evidence 均已保留。');
      await loadMemoryPage(memoryPage, requestSelection);
    } catch (error) {
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      reportError(error, '#memoryMaintenanceHistory');
    }
  }

  async function rollbackMemoryQuality(resolutionId) {
    const requestKey = lastMaintenanceHistorySelectionKey;
    const requestGeneration = maintenanceRequestGeneration;
    const requestSelection = selectionSnapshot();
    const operation = Number(resolutionId);
    if (!Number.isInteger(operation) || operation <= 0 || !requestKey ||
        !selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
    const selection = maintenanceSelection();
    if (!(await confirmAction('撤销这次语义等价整理？冗余 Atom 将恢复为 active；不会删除 Atom 或 Evidence。'))) return;
    if (!maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
    try {
      await post(ENDPOINTS.memoryQualityRollback, {
        ...selection, resolution_id: operation,
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      await loadMemoryMaintenanceHistory();
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      notice('语义等价整理已回滚；Atom 与 Evidence 均已保留。');
      await loadMemoryPage(memoryPage, requestSelection);
    } catch (error) {
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      reportError(error, '#memoryMaintenanceHistory');
    }
  }

  async function openConflictAtom(atomId) {
    const snapshot = selectionSnapshot();
    const id = Number(atomId);
    if (!Number.isInteger(id) || id <= 0) return;
    memoryView = 'atomic';
    syncMemoryViewButtons();
    syncMemoryViews();
    if (!selectionIsCurrent(snapshot)) return;
    await openMemoryDetail(id, snapshot);
  }

  function maintenanceActionMarkup(action) {
    const status = value => STATUS_LABELS[value] || value || 'unknown（未知）';
    return `<article class="maintenance-row" data-maintenance-atom="${text(action.atom_id)}">
      <div><strong>ATOM #${text(action.atom_id)}</strong><span>${text(action.summary || '未命名原子')}</span><small>${text(action.type_path || 'unknown')} · Subject（主题） ${text(action.subject_key || '—')}</small></div>
      <div class="maintenance-row-meta"><b>${text(status(action.from_status))} → ${text(status(action.to_status))}</b><small>TTL ${text(formatTimestamp(action.expires_at))} · ${text(action.reason_code || '')}</small></div>
    </article>`;
  }

  function maintenanceWarningMarkup(finding) {
    const relatedIds = Array.isArray(finding.related_atom_ids) ? finding.related_atom_ids : [];
    const isDuplicatePreference = finding.category === 'duplicate_active_preference_content';
    const related = isDuplicatePreference && relatedIds.length
      ? `<div class="maintenance-related-review"><small>打开 Atomic Lens 人工检查：</small>${relatedIds.map(atomId => `<button type="button" class="maintenance-atom-review" data-maintenance-review-atom="${text(atomId)}" aria-label="打开 Atomic Lens 人工检查 · ATOM #${text(atomId)}">ATOM #${text(atomId)}</button>`).join('')}</div>`
      : relatedIds.length
        ? ` · related Atom ${text(relatedIds.join(', '))}`
        : '';
    return `<article class="maintenance-row maintenance-warning" data-maintenance-warning="${text(finding.category || '')}">
      <div><strong>ATOM #${text(finding.atom_id)}</strong><span>${text(finding.summary || '未命名原子')}</span><small>${text(finding.type_path || 'unknown')} · Subject（主题） ${text(finding.subject_key || '—')}</small>${isDuplicatePreference ? related : ''}</div>
      <div class="maintenance-row-meta"><b>${text(maintenanceWarningLabel(finding.category))}</b><small>${text(finding.severity || 'warning')}${isDuplicatePreference ? '' : related}</small></div>
    </article>`;
  }

  function renderMaintenancePreview(payload, selectionKey = maintenanceSelectionKey(), selection = selectionSnapshot()) {
    const preview = payload || {};
    lastMaintenancePreview = preview;
    lastMaintenancePreviewSelectionKey = selectionKey;
    const actions = Array.isArray(preview.actions) ? preview.actions : [];
    const warnings = Array.isArray(preview.warnings) ? preview.warnings : [];
    const byPipeline = Object.entries(preview.applyable_by_pipeline || {});
    const byCategory = Object.entries(preview.warnings_by_category || {});
    const summary = [
      ['Preview time（预览时间）', preview.as_of || '—'],
      ['Scanned Atoms（已扫描原子）', preview.scanned_count ?? 0],
      ['Applyable Expiry（可执行过期）', preview.applyable_count ?? 0],
      ['Quality Warnings（质量提示）', preview.warning_count ?? 0],
    ];
    $('#memoryMaintenancePreview').innerHTML = `<div class="maintenance-summary">${summary.map(([label, value]) => `<div class="maintenance-summary-card"><span>${text(label)}</span><strong>${text(value)}</strong></div>`).join('')}</div><p class="maintenance-breakdown">${byPipeline.length ? `可执行分布：${byPipeline.map(([key, count]) => `${text(PIPELINE_LABELS[key] || key)} ${text(count)}`).join(' · ')}` : '当前没有可执行的 TTL 过期变更。'}${byCategory.length ? `<br>质量提示：${byCategory.map(([key, count]) => `${text(maintenanceWarningLabel(key))} ${text(count)}`).join(' · ')}` : ''}</p>`;
    const actionsNotice = preview.actions_truncated
      ? '<p class="maintenance-bounded">当前仅显示前 200 条可执行变更；应用仍会校验完整计划。</p>'
      : '';
    const warningsNotice = preview.warnings_truncated
      ? '<p class="maintenance-bounded">当前仅显示前 200 条质量提示。</p>'
      : '';
    $('#memoryMaintenanceActions').innerHTML = actionsNotice + (actions.map(maintenanceActionMarkup).join('') || core.stateMarkup('empty', '当前 Preview 没有可执行的确定性过期变更。'));
    $('#memoryMaintenanceWarnings').innerHTML = warningsNotice + (warnings.map(maintenanceWarningMarkup).join('') || core.stateMarkup('empty', '当前 Preview 没有质量提示。'));
    $('#memoryMaintenanceWarnings').innerHTML = $('#memoryMaintenanceWarnings').innerHTML.replace(/<button\b(?![^>]*\bdata-selection-generation=)/g,
      `<button data-selection-generation="${text(selection.generation)}"`);
    $('#memoryMaintenanceActionCount').textContent = String(preview.applyable_count ?? 0);
    $('#memoryMaintenanceWarningCount').textContent = String(preview.warning_count ?? 0);
    syncMaintenanceApplyButton();
  }

  function maintenanceSelection() {
    return {
      identity_token: selectedIdentityToken,
      partition_token: selectedPartitionToken,
    };
  }

  function maintenanceSelectionKey(selection = maintenanceSelection()) {
    return JSON.stringify({
      identity_token: String(selection.identity_token || ''),
      partition_token: String(selection.partition_token || ''),
    });
  }

  function maintenanceRequestIsCurrent(requestKey, requestGeneration) {
    return requestGeneration === maintenanceRequestGeneration &&
      requestKey === maintenanceSelectionKey();
  }

  function isMaintenancePlanStale(error) {
    return String(error?.message || error?.code || '').includes('maintenance_plan_stale');
  }

  async function loadMemoryMaintenancePreview() {
    if (!selectedIdentityToken && !selectedPartitionToken) {
      clearMaintenancePreview('请选择操作对象后生成维护 Preview。');
      return;
    }
    clearMaintenancePreview('正在生成维护 Preview……');
    const selection = maintenanceSelection();
    const requestKey = maintenanceSelectionKey(selection);
    const requestGeneration = maintenanceRequestGeneration;
    const request = beginSelectionRequest('maintenance-preview');
    if (!request) return;
    try {
      const payload = await get(ENDPOINTS.memoryMaintenancePreview, selection);
      if (!selectionRequestIsCurrent(request) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      renderMaintenancePreview(payload, requestKey, request);
    } catch (error) {
      if (!selectionRequestIsCurrent(request) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      reportError(error, '#memoryMaintenancePreview');
    }
  }

  async function applyMemoryMaintenance() {
    const preview = lastMaintenancePreview;
    if (!preview?.plan_token || !Number(preview.applyable_count || 0)) return;
    const requestKey = lastMaintenancePreviewSelectionKey;
    const requestGeneration = maintenanceRequestGeneration;
    const requestSelection = selectionSnapshot();
    if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
    const selection = maintenanceSelection();
    const count = Number(preview.applyable_count || 0);
    const confirmed = await confirmAction(`将 ${count} 条 active（活跃）记忆标记为 expired（已过期）。不会删除 Atom 或 Evidence，并会写入 Lifecycle Event（生命周期事件）。是否继续？`);
    if (!confirmed) return;
    if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
    try {
      const result = await post(ENDPOINTS.memoryMaintenanceApply, {
        ...selection,
        as_of: preview.as_of,
        plan_token: preview.plan_token,
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      renderMaintenancePreview(result.new_preview || result, requestKey, requestSelection);
      notice(`已应用 ${text(result.applied_count ?? count)} 条确定性生命周期变更。`);
      loadMemoryPage(memoryPage, requestSelection).catch(error => reportError(error, '#memoryList'));
    } catch (error) {
      if (!selectionIsCurrent(requestSelection) || !maintenanceRequestIsCurrent(requestKey, requestGeneration)) return;
      if (isMaintenancePlanStale(error)) {
        clearMaintenancePreview('数据已变化，请重新生成 Preview。');
        notice('数据已变化，请重新生成 Preview。', 'error');
        return;
      }
      reportError(error, '#memoryMaintenancePreview');
    }
  }

  function renderMemoryDetail(payload, selection = selectionSnapshot()) {
    const detail = payload || {};
    const atom = detail.atom || {};
    const evidence = Array.isArray(detail.evidence) ? detail.evidence : [];
    const lifecycle = Array.isArray(detail.lifecycle) ? detail.lifecycle : [];
    const lifecyclePresentation = detail.lifecycle_presentation || {};
    const related = Array.isArray(detail.related_by_subject) ? detail.related_by_subject : [];
    const embeddings = Array.isArray(detail.embeddings) ? detail.embeddings : [];
    const statusText = value => text(STATUS_LABELS[value] || value || 'unknown（未知）');
    const canForget = ['active', 'dormant', 'expired'].includes(String(atom.status || '')) &&
      !String(atom.type_path || '').startsWith('cognition.');
    const forgetAction = canForget
      ? `<button type="button" class="deny" data-forget-atom="${text(atom.id)}" data-selection-generation="${text(selection.generation)}">停止召回这条记忆</button>`
      : '';
    const canCorrect = atom.status === 'active' &&
      (String(atom.type_path || '').startsWith('fact.preference.') ||
        ['anchor.date.birthday', 'anchor.date.anniversary'].includes(String(atom.type_path || '')));
    const correctionAction = canCorrect
      ? `<section class="correction-control"><label class="field"><span>更正内容</span><textarea id="memoryCorrectionContent" data-correction-content maxlength="2000" placeholder="输入新的 current memory（当前记忆）内容"></textarea></label><button type="button" data-correct-atom="${text(atom.id)}" data-selection-generation="${text(selection.generation)}">更正这条记忆</button><small>将创建新的当前记忆并保留旧版本及其证据，不会修改原聊天记录。</small></section>`
      : '';
    $('#memoryDetail').hidden = false;
    $('#memoryDetail').classList.add('is-open');
    $('#memoryDetail').dataset.selectionGeneration = String(selection.generation);
    $('#memoryDetail').innerHTML = `<div class="detail-head"><div><p class="kicker">Atomic Lens（原子透镜） · Side Atomic Lens（侧边原子透镜） · ATOM #${text(atom.id)}</p><h3>${text(atom.summary_title || atom.content || '原子记忆详情')}</h3></div>${forgetAction}<button type="button" class="button-secondary" data-close-memory data-selection-generation="${text(selection.generation)}">关闭</button></div>
      <p class="detail-content">${text(atom.content || '')}</p>
      ${correctionAction}
      <dl class="metrics"><div><dt>Type Path（记忆类型）</dt><dd>${text(atom.type_label || atom.type_path)}</dd></div><div><dt>Subject（主题）</dt><dd>${text(atom.subject_key || '—')}</dd></div><div><dt>Status（状态）</dt><dd>${text(atom.status_label || STATUS_LABELS[atom.status] || atom.status)}</dd></div><div><dt>可检索</dt><dd>${atom.retrieval_eligible ? '是' : '否'}</dd></div><div><dt>importance（重要度）</dt><dd>${percent(atom.importance)}</dd></div><div><dt>confidence（置信度）</dt><dd>${percent(atom.confidence)}</dd></div></dl>
      <h4>Evidence（证据）</h4><p class="meta">${text(detail.provenance?.kind || 'online_atomic')} · ${text(detail.provenance?.migration_epoch || '非迁移来源')} · ${text(detail.embedding_status || 'Embedding（向量索引）未知')}</p>
      <ul class="detail-list">${evidence.map(row => `<li><strong>证据 #${text(row.id)}</strong><span>${text(row.source_excerpt || '无摘要')}</span><small>${text(formatTimestamp(row.created_at))} · hash ${text(row.source_hash)}</small></li>`).join('') || '<li>暂无证据</li>'}</ul>
      <h4>Lifecycle（生命周期）</h4>${lifecyclePresentation.kind === 'baseline'
        ? `<div class="lifecycle-baseline"><strong>Created（创建）</strong><span>${text(formatTimestamp(lifecyclePresentation.created_at || atom.created_at))}</span><p>${text(lifecyclePresentation.message)}</p><small>Current status（当前状态）: ${text(lifecyclePresentation.current_status || atom.status_label)}</small></div>`
        : lifecyclePresentation.kind === 'unavailable'
          ? `<div class="lifecycle-baseline"><strong>${text(lifecyclePresentation.current_status || atom.status_label)}</strong><p>${text(lifecyclePresentation.message)}</p></div>`
          : `<ul class="detail-list">${lifecycle.map(row => `<li><strong>${statusText(row.from_status || '—')} → ${statusText(row.to_status)}</strong><span>${text(row.reason_code)}</span><small>${text(formatTimestamp(row.created_at))}</small></li>`).join('')}</ul>`}
      <h4>Embedding（向量索引）</h4><ul class="detail-list">${embeddings.map(row => `<li><strong>${text(row.provider_id)} / ${text(row.model_name)}</strong><span>${text(row.status)} · ${text(row.dimensions)} 维</span><small>content hash ${text(row.content_hash)}</small></li>`).join('') || '<li>尚未建立有效向量索引</li>'}</ul>
      <h4>Same Subject（同主题原子）</h4><p class="meta">${text(detail.related_by_subject_note || '按共同主题聚类，不代表 Knowledge Graph 关系')}</p><div class="sibling-list">${related.map(item => `<button type="button" class="sibling-chip" data-memory-id="${text(item.id)}" data-selection-generation="${text(selection.generation)}"><strong>ATOM #${text(item.id)}</strong><span>${text(item.content || item.summary_title)}</span><small>${text(item.status_label || item.status)}</small></button>`).join('') || '<span class="muted">暂无同主题原子</span>'}</div>`;
  }

  function renderPortraitDirectory(items, selection = selectionSnapshot()) {
    const root = $('#portraitDirectory');
    if (!root) return;
    const listRoot = root.querySelector('.portrait-directory-list');
    if (!listRoot) return;
    const cards = Array.isArray(items) ? items : [];
    const list = cards.map(item => `<button type="button" class="portrait-card ${item.is_current_identity ? 'is-current' : ''}" data-partition-token="${text(item.partition_token)}" data-selection-generation="${text(selection.generation)}" aria-label="${text(item.safe_label)}">
      <span class="portrait-card-orbit" aria-hidden="true">✦</span><span class="portrait-card-main"><strong>${text(item.safe_label)}</strong><small>${item.is_current_identity ? 'Current UMO（当前会话身份） · ' : ''}${item.is_known_identity ? 'Known identity（已知身份）' : 'Memory-only partition（仅记忆分区）'}</small></span>
      <span class="portrait-card-stats"><b>${text(item.active_count)} active（活跃）</b><small>${text(item.core_portrait_count)} core（核心） · ${text(item.dynamic_count)} current（当前） · Native（原生） ${text(item.native_count)} / Historical（历史） ${text(item.historical_count)}</small><small>最近更新 ${text(item.latest_updated_at ? formatTimestamp(item.latest_updated_at) : '未知')}</small></span>
    </button>`).join('');
    listRoot.innerHTML = list || '<p class="muted">暂无可见 Atomic identity partition（身份分区）。</p>';
  }

  const profileItemMarkup = (items, selection = selectionSnapshot()) => (Array.isArray(items) ? items : []).map(item => `<button type="button" class="profile-item" data-memory-id="${text(item.atom_id)}" data-selection-generation="${text(selection.generation)}"><strong>${text(item.content)}</strong><small>${text(item.type_label || item.type_path)} · Subject（主题）: ${text(item.subject_key || '无主题')} · confidence（置信度） ${percent(item.confidence)}</small></button>`).join('');

  function renderProfile(payload, selection = selectionSnapshot()) {
    currentProfilePayload = payload || {};
    const stable = payload?.stable || {};
    const changing = payload?.changing || {};
    const projection = payload?.projection || {};
    const metrics = payload?.metrics || {};
    const topics = Array.isArray(payload?.topics) ? payload.topics : [];
    const section = (title, items, key, open = false) => {
      const values = Array.isArray(items) ? items : [];
      const visible = values.slice(0, 8);
      const total = key === 'recent'
        ? values.length
        : Number(payload?.projection_counts?.[key] ?? values.length);
      const more = total > visible.length
        ? `<button type="button" class="show-all" data-profile-section-load="${text(key)}" data-profile-section-page="0" data-selection-generation="${text(selection.generation)}">Show all / Load more（加载更多） · ${text(total)}</button>`
        : '';
      return `<details class="profile-group" data-profile-group="${text(key)}" ${open ? 'open' : ''}><summary><span>${text(title)}</span><b>${text(total)}</b></summary><div class="profile-group-items" data-profile-section="${text(key)}" data-selection-generation="${text(selection.generation)}" data-profile-section-page="0" data-profile-section-page-size="8" data-profile-section-total="${text(total)}">${profileItemMarkup(visible, selection) || '<p class="muted">暂无可追踪原子</p>'}${more}</div></details>`;
    };
    const metricCards = [['Active Atoms（活跃原子）', metrics.active_atom_count], ['Stable（稳定信息）', metrics.stable_count], ['Changing（动态信息）', metrics.changing_count], ['Subjects（主题）', metrics.subject_count], ['高置信度', metrics.high_confidence_count], ['最近更新', metrics.recently_updated_count]];
    const composition = [['core', 'Core Portrait（核心画像）', 'core'], ['current_context', 'Current Context（当前上下文）', 'current_context'], ['cognition', 'Cognition（互动认知）', 'cognition'], ['archive', 'Context Archive（背景记忆）', 'archive']];
    const total = Math.max(1, Number(metrics.active_atom_count || 0));
    const ring = composition.map(([key, label], index) => {
      const value = Number(payload?.projection_counts?.[key] || 0);
      const angle = Math.round((value / total) * 360);
      return `<span class="portrait-ring-segment segment-${index}" style="--segment-angle:${angle}deg" title="${text(label)}: ${value}"></span>`;
    }).join('');
    const topicMax = Math.max(1, ...topics.map(item => Number(item.atom_count || 0)));
    const topicMarkup = topics.slice(0, 12).map(item => `<button type="button" class="topic-item" data-profile-topic="${text(item.subject_key)}" data-subject-filter="${text(item.subject_key)}" data-selection-generation="${text(selection.generation)}"><span><strong>${text(item.subject_key || '未命名主题')}</strong><small>${text(item.atom_count)} atoms（原子） · ${text(item.preference_count)} 喜欢 · ${text(item.dislike_count)} 不喜欢</small></span><i style="--topic-size:${Math.round(Number(item.atom_count || 0) / topicMax * 100)}%"></i></button>`).join('') || '<p class="muted">暂无主题聚合</p>';
    $('#profileContent').innerHTML = `<section class="portrait-hero"><div class="portrait-core"><span class="portrait-orbit">✦</span><strong>Memory Portrait（记忆画像）</strong><small>基于当前身份的 Atomic corpus（原子记忆集合）</small><small>Likes（喜欢） · Dislikes（不喜欢）</small></div><div class="portrait-composition"><div class="portrait-ring" aria-label="Memory Composition（记忆构成）">${ring}<span>${text(metrics.active_atom_count || 0)}<small>active atoms（活跃原子）</small></span></div><div><h3>Memory Composition（记忆构成）</h3><p class="hint">这是记忆构成，不是性格评分或心理画像。</p><div class="composition-legend">${composition.map(([, label]) => `<span><b></b>${text(label)}</span>`).join('')}</div></div></div></section><div class="profile-metrics">${metricCards.map(([label, value]) => `<div class="profile-metric"><span>${text(label)}</span><strong>${text(value ?? 0)}</strong></div>`).join('')}</div><div class="accordion-tools"><button type="button" data-accordion="expand">Expand all（全部展开）</button><button type="button" data-accordion="collapse">Collapse all（全部折叠）</button></div><section class="topic-constellation"><div class="profile-section-heading"><h3>Topic（主题） constellation</h3><small>点击主题可回到 Memory Explorer（原子记忆浏览）筛选</small></div>${topicMarkup}</section><div class="profile-projection"><p class="kicker">PORTRAIT PROJECTION（画像投影）</p>${section('Core Portrait（核心画像）', projection.core || stable.preferences.concat(stable.dislikes || [], stable.facts || []), 'core', true)}${section('Current Context（当前上下文） · Current States（当前状态）', projection.current_context || changing.states.concat(changing.open_loops || []), 'current_context', true)}${section('Cognition（互动认知）', projection.cognition || changing.cognition, 'cognition', false)}${section(`Context Archive（背景记忆） · ${text(payload?.archive_count || 0)}`, projection.archive || [], 'archive', false)}</div><section class="recent-changes"><div class="profile-section-heading"><h3>Recent Changes（最近变化）</h3><small>按 created_at / updated_at 确定性排序，可追踪到 Atom</small></div>${section('', payload.recent_changes || [], 'recent', false)}</section>`;
  }

  function renderOverview(payload, selection) {
    const atoms = payload?.atoms || {};
    const embedding = payload?.embedding || {};
    const cards = [
      ['Atomic 总数', atoms.total ?? 0], ['可检索', atoms.retrieval_eligible ?? 0],
      ['证据', payload?.evidence_total ?? 0], ['Embedding 覆盖', percent(embedding.coverage)],
      ['认知 Atom', payload?.cognition_atoms ?? 0], ['未完成事项', payload?.open_loop_atoms ?? 0],
    ];
    const pipeline = payload?.pipeline_counts || {};
    const taxonomy = Array.isArray(payload?.taxonomy) ? payload.taxonomy : [];
    if (taxonomy.length && $('#memoryType')) {
      const current = $('#memoryType').value;
      $('#memoryType').innerHTML = '<option value="">全部类型</option>' + taxonomy.map(group => `<optgroup label="${text(group.key)}（${text(group.label)}）"><option value="${text(group.key)}.">${text(group.label)} · ${text(group.key)}.*</option>${(group.types || []).map(item => `<option value="${text(item.key)}">${text(item.label)} · ${text(item.key)}</option>`).join('')}</optgroup>`).join('');
      $('#memoryType').value = current;
    }
    $('#memoryCompositionStrip').innerHTML = Object.entries(PIPELINE_LABELS).map(([key, label]) => `<button type="button" class="composition-chip" data-pipeline-filter="${text(key)}" data-selection-generation="${text(selection.generation)}"><span>${text(label)}</span><strong>${text(pipeline[key] ?? 0)}</strong></button>`).join('');
    $('#memoryOverview').innerHTML = cards.map(([label, value]) => `<div class="overview-metric"><span>${text(label)}</span><strong>${text(value)}</strong></div>`).join('');
  }

  const ENDPOINTS = { settings: 'evolution/settings', reviews: 'cognition/reviews', versions: 'persona/versions', personaTimeline: 'persona/timeline', personaVersionDetail: 'persona/version-detail', approve: 'cognition/approve', reject: 'cognition/reject', rollback: 'persona/rollback', remember: 'webui/remember-identity', partitions: 'profile/partitions', memoryList: 'memory/list', memoryStructure: 'memory/structure', memoryDetail: 'memory/detail', memoryForget: 'memory/forget', memoryCorrection: 'memory/correction', memoryRemember: 'memory/remember', memoryMaintenancePreview: 'memory/maintenance/preview', memoryMaintenanceApply: 'memory/maintenance/apply', memoryMaintenanceHistory: 'memory/maintenance/history', memoryMaintenanceRollback: 'memory/maintenance/rollback', memoryConflictRollback: 'memory/maintenance/conflict-rollback', memoryQualityRollback: 'memory/maintenance/quality-rollback', profile: 'profile/current', profileSection: 'profile/section', overview: 'system/memory-overview' };

  async function loadPersonaGrowth(page = growthTimelinePage, expectedSelection = selectionSnapshot()) {
    const request = beginSelectionRequest('persona-timeline', expectedSelection);
    if (!request) return false;
    growthTimelinePage = Math.max(1, Number(page) || 1);
    if (!request.identityToken) {
      renderCurrentPersona(null);
      renderGrowthTimeline({ timeline: [], pagination: { page: 1, page_count: 1, total: 0 } }, request);
      return false;
    }
    renderState('#growthTimeline', 'loading', '正在读取人格成长时间线……');
    try {
      const payload = await get(ENDPOINTS.personaTimeline, {
        identity_token: request.identityToken,
        page: growthTimelinePage,
        page_size: 30,
      });
      if (!selectionRequestIsCurrent(request)) return false;
      renderCurrentPersona(payload);
      renderGrowthTimeline(payload, request);
      return true;
    } catch (error) {
      if (!selectionRequestIsCurrent(request)) return false;
      renderState('#growthCurrentPersona', 'error', '当前人格读取失败，请稍后重试。', true);
      renderState('#growthTimeline', 'error', String(error?.message || '人格成长记录读取失败，请稍后重试。'), true);
      return false;
    }
  }

  async function loadPersonaVersionDetail(versionId, page = 1, expectedSelection = selectionSnapshot()) {
    const request = beginSelectionRequest('persona-version-detail', expectedSelection);
    if (!request || !request.identityToken || !Number(versionId)) return false;
    openGrowthVersionId = Number(versionId);
    growthEvidencePage = Math.max(1, Number(page) || 1);
    $('#personaVersionDetail').hidden = false;
    renderState('#personaVersionDetail', 'loading', '正在读取人格版本详情……');
    const payload = await get(ENDPOINTS.personaVersionDetail, {
      identity_token: request.identityToken,
      version_id: openGrowthVersionId,
      evidence_page: growthEvidencePage,
      evidence_page_size: 10,
    }).catch(error => ({ __error: error }));
    if (!selectionRequestIsCurrent(request)) return false;
    if (payload?.__error) {
      renderState('#personaVersionDetail', 'error', String(payload.__error?.message || '版本详情读取失败，请重试。'), true);
      return false;
    }
    renderVersionDetail(payload, request);
    return true;
  }
  let memoryPage = 1;
  const memoryFilterKey = (values = {}) => JSON.stringify({
    identity_token: values.identity_token ?? selectedIdentityToken,
    partition_token: values.partition_token ?? selectedPartitionToken,
    status: values.status ?? $('#memoryStatus')?.value ?? '',
    type_prefix: values.type_prefix ?? $('#memoryType')?.value ?? '',
    subject_key: values.subject_key ?? $('#memorySubject')?.value ?? '',
    search: values.search ?? $('#memorySearch')?.value ?? '',
  });

  function syncMemoryViewButtons() {
    document.querySelectorAll('[data-memory-view]').forEach(item => {
      const active = item.dataset.memoryView === memoryView;
      item.classList.toggle('active', active);
      item.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
  }

  async function setMemoryView(view) {
    memoryView = view === 'atomic' ? 'atomic' : view === 'maintenance' ? 'maintenance' : 'list';
    syncMemoryViewButtons();
    syncMemoryViews();
    if (memoryView === 'atomic' && lastStructureFilterKey !== memoryFilterKey()) {
      await loadMemoryStructure(1);
    }
    if (memoryView === 'maintenance') await loadMemoryMaintenanceHistory();
  }

  async function loadMemoryPage(page = memoryPage, expectedSelection = null) {
    const selection = expectedSelection ? { ...expectedSelection } : selectionSnapshot();
    const listRequest = beginSelectionRequest('memory-list', selection);
    const structureRequest = beginSelectionRequest('memory-structure', selection);
    if (!listRequest || !structureRequest || (!selection.identityToken && !selection.partitionToken)) return false;
    const requestedPage = Math.max(1, Number(page) || 1);
    memoryPage = requestedPage;
    structurePage = 1;
    renderState('#memoryList', 'loading', '正在读取 Atomic 记忆……');
    $('#memoryPager').innerHTML = '';
    $('#atomicStructureSummary').innerHTML = '';
    renderState('#atomicStructureList', 'loading', '正在读取 Atomic 结构……');
    $('#atomicStructurePager').innerHTML = '';
    const params = {
      ...selectionParameters(selection), page: requestedPage, page_size: 24,
      status: $('#memoryStatus').value, type_prefix: $('#memoryType').value,
      subject_key: $('#memorySubject').value,
      search: $('#memorySearch').value, sort: $('#memorySort').value,
      direction: $('#memoryDirection').value,
    };
    const structureParams = {
      ...selectionParameters(selection),
      page: 1, page_size: 20, status: params.status, type_prefix: params.type_prefix,
      subject_key: params.subject_key, search: params.search,
    };
    const structureKey = memoryFilterKey(params);
    const [listResult, structureResult] = await Promise.allSettled([
      get(ENDPOINTS.memoryList, params), get(ENDPOINTS.memoryStructure, structureParams),
    ]);
    if (selectionRequestIsCurrent(listRequest)) {
      if (listResult.status === 'fulfilled') renderMemoryList(listResult.value, listRequest);
      else reportError(listResult.reason, '#memoryList');
    }
    if (selectionRequestIsCurrent(structureRequest) && structureResult.status === 'fulfilled') {
      lastStructureFilterKey = structureKey;
      renderAtomicStructure(structureResult.value, structureRequest);
    } else if (selectionRequestIsCurrent(structureRequest)) {
      reportError(structureResult.reason, '#atomicStructureList');
    }
    return selectionRequestIsCurrent(listRequest);
  }

  async function loadMemoryStructure(page = structurePage) {
    const request = beginSelectionRequest('memory-structure');
    if (!request || (!request.identityToken && !request.partitionToken)) return false;
    const requestedPage = Math.max(1, Number(page) || 1);
    structurePage = requestedPage;
    $('#atomicStructureSummary').innerHTML = '';
    renderState('#atomicStructureList', 'loading', '正在读取 Atomic 结构……');
    $('#atomicStructurePager').innerHTML = '';
    const params = {
      ...selectionParameters(request),
      page: requestedPage, page_size: 20, status: $('#memoryStatus').value,
      type_prefix: $('#memoryType').value, subject_key: $('#memorySubject').value,
      search: $('#memorySearch').value,
    };
    const filterKey = memoryFilterKey(params);
    try {
      const payload = await get(ENDPOINTS.memoryStructure, params);
      if (!selectionRequestIsCurrent(request)) return false;
      renderAtomicStructure(payload, request);
      lastStructureFilterKey = filterKey;
      return true;
    }
    catch (error) {
      if (!selectionRequestIsCurrent(request)) return false;
      reportError(error, '#atomicStructureList');
      return false;
    }
  }

  async function loadProfile(expectedSelection = selectionSnapshot()) {
    const request = beginSelectionRequest('profile', expectedSelection);
    if (!request || (!request.identityToken && !request.partitionToken)) return false;
    invalidateSelectionRequests('profile-section:');
    renderState('#profileContent', 'loading', '正在生成当前身份画像……');
    try {
      const payload = await get(ENDPOINTS.profile, selectionParameters(request));
      if (!selectionRequestIsCurrent(request)) return false;
      renderProfile(payload, request);
      return true;
    } catch (error) {
      if (!selectionRequestIsCurrent(request)) return false;
      reportError(error, '#profileContent');
      return false;
    }
  }

  async function loadProfileSection(group, key, expectedSelection = selectionFromElement(group)) {
    const request = beginSelectionRequest(`profile-section:${String(key || '')}`, expectedSelection);
    if (!group || !key || !request) return false;
    const currentPage = Number(group.dataset.profileSectionPage || 0);
    const nextPage = currentPage + 1;
    const pending = group.querySelector('[data-profile-section-loading]');
    if (pending) pending.remove();
    group.insertAdjacentHTML('beforeend', '<p class="muted" data-profile-section-loading>正在加载画像条目……</p>');
    try {
      const payload = await get(ENDPOINTS.profileSection, {
        ...selectionParameters(request),
        section: key,
        page: nextPage,
        page_size: 20,
      });
      if (!selectionRequestIsCurrent(request)) return false;
      const loading = group.querySelector('[data-profile-section-loading]');
      if (loading) loading.remove();
      const items = Array.isArray(payload?.items) ? payload.items : [];
      const total = Number(payload?.total || 0);
      const previousButton = group.querySelector('[data-profile-section-load]');
      if (previousButton) previousButton.remove();
      if (currentPage === 0) {
        group.innerHTML = profileItemMarkup(items, request);
      } else if (items.length) {
        group.insertAdjacentHTML('beforeend', profileItemMarkup(items, request));
      }
      group.dataset.profileSectionPage = String(payload?.page || nextPage);
      group.dataset.profileSectionPageSize = String(payload?.page_size || 20);
      group.dataset.profileSectionTotal = String(total);
      const loaded = group.querySelectorAll('.profile-item').length;
      if (loaded < total) {
        group.insertAdjacentHTML('beforeend', `<button type="button" class="show-all" data-profile-section-load="${text(key)}" data-selection-generation="${text(request.generation)}" data-profile-section-page="${text(payload?.page || nextPage)}">Show all / Load more（加载更多） · ${text(total - loaded)}</button>`);
      }
      return true;
    } catch (error) {
      if (!selectionRequestIsCurrent(request)) return false;
      reportError(error, '#profileContent');
      return false;
    }
  }

  async function loadOverview(expectedSelection = selectionSnapshot()) {
    const request = beginSelectionRequest('overview', expectedSelection);
    if (!request || (!request.identityToken && !request.partitionToken)) return false;
    renderState('#memoryOverview', 'loading', '正在读取当前身份概览……');
    renderState('#memoryCompositionStrip', 'loading', '正在读取当前身份记忆构成……');
    try {
      const payload = await get(ENDPOINTS.overview, selectionParameters(request));
      if (!selectionRequestIsCurrent(request)) return false;
      renderOverview(payload, request);
      return true;
    } catch (error) {
      if (!selectionRequestIsCurrent(request)) return false;
      reportError(error, '#memoryOverview');
      renderState('#memoryCompositionStrip', 'error', '当前身份记忆构成读取失败，可点击重试。', true, request);
      return false;
    }
  }

  function renderSettings(settings) {
    if (!settings) return;
    const branches = Array.isArray(settings.branches) ? settings.branches : [];
    const previousIdentityToken = selectedIdentityToken;
    $('#identity').innerHTML = '<option value="">请选择操作对象</option>' + branches.map(branch =>
      `<option value="${text(branch.identity_token)}">${text(branch.label)}</option>`
    ).join('');
    setSelection(settings.identity_token, selectedPartitionToken);
    if (previousIdentityToken !== selectedIdentityToken && lastMaintenancePreview) {
      clearMaintenancePreview('操作对象已变化，请重新生成 Preview。');
    }
    if (previousIdentityToken !== selectedIdentityToken) {
      clearRememberForm();
      clearMaintenanceHistory();
    }
    $('#identity').value = selectedIdentityToken;
    const route = document.querySelector(`input[name=route][value="${settings.scope_mode}"]`);
    if (route) route.checked = true;
    if (settings.autonomy_profile) $('#mode').value = settings.autonomy_profile;
    if (settings.plasticity !== undefined) {
      $('#plasticity').value = settings.plasticity;
      $('#plasticityValue').value = settings.plasticity;
    }
    const identity = settings.identity;
    $('#scopeContext').textContent = settings.identity_available && identity
      ? `当前分支：${identity.bot_id} / ${identity.persona_name} / ${settings.scope_mode === 'global_persona' ? '全部用户' : identity.user_id}`
      : '当前分支：等待 AstrBot 会话身份';
    $('#identityState').textContent = settings.identity_available && identity
      ? `${identity.persona_name} / ${identity.user_id}`
      : '尚未选择';
    $('#identitySummary').textContent = settings.identity_available && identity
      ? `${identity.bot_id} · ${identity.persona_name}`
      : '等待 AstrBot 会话身份';
    $('#personaSummary').innerHTML = settings.identity_available && identity
      ? `<dl class="persona-grid">
          <div><dt>人格</dt><dd>${text(identity.persona_name)}</dd></div>
          <div><dt>Bot</dt><dd>${text(identity.bot_id)}</dd></div>
          <div><dt>演化路线</dt><dd>${text(settings.scope_mode)}</dd></div>
          <div><dt>人格自治档位</dt><dd>${text(settings.autonomy_profile || 'balanced')}</dd></div>
        </dl>`
      : core.stateMarkup('empty', '请选择操作对象后查看当前人格分支。');
  }

  async function load(expectedSelection = selectionSnapshot()) {
    const initialRequest = beginSelectionRequest('bootstrap', expectedSelection);
    if (!initialRequest) return false;
    if (!initialRequest.identityToken && initialRequest.partitionToken) {
      renderReviews([]);
      renderVersions([]);
      renderCurrentPersona(null);
      renderGrowthTimeline({ timeline: [], pagination: { page: 1, page_count: 1, total: 0 } }, initialRequest);
      renderVersionDetail(null, initialRequest);
      const reads = [
        loadMemoryPage(1, initialRequest), loadProfile(initialRequest),
        loadOverview(initialRequest), loadPartitions(initialRequest),
      ];
      if (memoryView === 'maintenance') reads.push(loadMemoryMaintenanceHistory());
      await Promise.allSettled(reads);
      return selectionIsCurrent(initialRequest) && selectionRequestIsCurrent(initialRequest);
    }
    const mayAdoptInitialIdentity = !initialRequest.identityToken;
    renderState('#reviews', 'loading', '正在读取认知候选……');
    renderState('#versions', 'loading', '正在读取版本历史……');
    const params = initialRequest.identityToken
      ? { identity_token: initialRequest.identityToken } : undefined;
    let settings;
    try {
      settings = await get(ENDPOINTS.settings, params);
      if (!selectionRequestIsCurrent(initialRequest)) return false;
      $('#apiState').textContent = '连接正常';
    } catch (error) {
      if (!selectionRequestIsCurrent(initialRequest)) return false;
      reportError(error, '#reviews');
      renderState('#versions', 'error', '人格设置读取失败，暂时无法读取版本。', true);
      return false;
    }
    renderSettings(settings);
    let loadRequest = initialRequest;
    if (mayAdoptInitialIdentity && !selectionIsCurrent(initialRequest) &&
        selectedIdentityToken === String(settings?.identity_token || '')) {
      loadRequest = beginSelectionRequest('bootstrap', selectionSnapshot());
    }
    if (!selectionRequestIsCurrent(loadRequest)) return false;
    if (!selectedIdentityToken) {
      renderReviews([]);
      renderVersions([]);
      renderCurrentPersona(null);
      renderGrowthTimeline({ timeline: [], pagination: { page: 1, page_count: 1, total: 0 } });
      renderVersionDetail(null);
      renderState('#profileContent', 'empty', '请选择操作对象后查看当前身份画像。');
      renderState('#memoryOverview', 'empty', '请选择操作对象后查看记忆概览。');
      renderMemoryList({ items: [], page: 1, page_size: 24, total: 0 });
      renderAtomicStructure({ subjects: [], summary: {}, page: 1, page_size: 20, total_subjects: 0 });
      await loadPartitions();
      return true;
    }
    const branchSelection = selectionSnapshot();
    const reviewsRequest = beginSelectionRequest('candidate-reviews', branchSelection);
    const versionsRequest = beginSelectionRequest('persona-versions', branchSelection);
    const branchParams = { identity_token: branchSelection.identityToken };
    const [reviews, versions] = await Promise.allSettled([
      get(ENDPOINTS.reviews, branchParams),
      get(ENDPOINTS.versions, branchParams),
    ]);
    if (!selectionIsCurrent(branchSelection) || !selectionRequestIsCurrent(loadRequest)) return false;
    if (selectionRequestIsCurrent(reviewsRequest)) {
      if (reviews.status === 'fulfilled') renderReviews(reviews.value, reviewsRequest);
      else reportError(reviews.reason, '#reviews');
    }
    if (selectionRequestIsCurrent(versionsRequest)) {
      if (versions.status === 'fulfilled') renderVersions(versions.value, versionsRequest);
      else reportError(versions.reason, '#versions');
    }
    await loadPersonaGrowth(1, branchSelection);
    if (!selectionIsCurrent(branchSelection) || !selectionRequestIsCurrent(loadRequest)) return false;
    await Promise.allSettled([
      loadMemoryPage(1, branchSelection), loadProfile(branchSelection),
      loadOverview(branchSelection), loadPartitions(branchSelection),
    ]);
    return selectionIsCurrent(branchSelection) && selectionRequestIsCurrent(loadRequest);
  }

  async function openMemoryDetail(atomId, expectedSelection = selectionSnapshot()) {
    const request = beginSelectionRequest('memory-detail', expectedSelection);
    const id = Number(atomId);
    if (!request || !Number.isInteger(id) || id <= 0 ||
        (!request.identityToken && !request.partitionToken)) return false;
    const root = $('#memoryDetail');
    root.hidden = false;
    root.classList.add('is-open');
    root.dataset.selectionGeneration = String(request.generation);
    renderState('#memoryDetail', 'loading', '正在读取 Atomic 记忆详情……', false, request);
    try {
      const payload = await get(ENDPOINTS.memoryDetail, selectionParameters(request, { atom_id: id }));
      if (!selectionRequestIsCurrent(request)) return false;
      renderMemoryDetail(payload, request);
      return true;
    } catch (error) {
      if (!selectionRequestIsCurrent(request)) return false;
      reportError(error, '#memoryDetail');
      return false;
    }
  }

  async function refreshMemoryProjections(selection) {
    await Promise.allSettled([
      loadProfile(selection), loadOverview(selection), loadPartitions(selection),
    ]);
    return selectionIsCurrent(selection);
  }

  async function forgetMemoryAtom(atomId, expectedSelection = selectionSnapshot()) {
    const requestSelection = { ...expectedSelection };
    const id = Number(atomId);
    if (!selectionIsCurrent(requestSelection) || !Number.isInteger(id) || id <= 0) return false;
    const confirmed = await confirmAction('此操作将把该 Atom 标记为 forgotten，停止正常记忆召回；不会物理删除原聊天、证据或审计记录，也不会自动修改已应用人格。');
    if (!confirmed || !selectionIsCurrent(requestSelection)) return false;
    try {
      await post(ENDPOINTS.memoryForget, {
        ...selectionParameters(requestSelection),
        atom_id: id,
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection)) return false;
      if (!await openMemoryDetail(id, requestSelection)) return false;
      if (!await loadMemoryPage(memoryPage, requestSelection)) return false;
      if (!await refreshMemoryProjections(requestSelection)) return false;
      if (!selectionIsCurrent(requestSelection)) return false;
      notice('该 Atom 已标记为 forgotten（已遗忘），不再参与正常召回');
      return true;
    } catch (error) {
      if (!selectionIsCurrent(requestSelection)) return false;
      reportError(error, '#memoryDetail');
      return false;
    }
  }

  async function correctMemoryAtom(atomId, expectedSelection = selectionSnapshot()) {
    const requestSelection = { ...expectedSelection };
    const id = Number(atomId);
    if (!selectionIsCurrent(requestSelection) || !Number.isInteger(id) || id <= 0) return false;
    const field = $('#memoryCorrectionContent');
    const content = String(field?.value || '').trim();
    if (!content) {
      notice('请输入更正内容', 'error');
      return;
    }
    const confirmed = await confirmAction('将创建新的当前记忆并保留旧版本及其证据，不会修改原聊天记录。确定提交更正吗？');
    if (!confirmed || !selectionIsCurrent(requestSelection)) return false;
    try {
      const result = await post(ENDPOINTS.memoryCorrection, {
        ...selectionParameters(requestSelection),
        atom_id: id,
        correction_content: content,
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection)) return false;
      if (!await openMemoryDetail(result.atom_id, requestSelection)) return false;
      if (!await loadMemoryPage(memoryPage, requestSelection)) return false;
      if (!await refreshMemoryProjections(requestSelection)) return false;
      if (!selectionIsCurrent(requestSelection)) return false;
      notice('更正已提交，新的当前记忆已生效');
      return true;
    } catch (error) {
      if (!selectionIsCurrent(requestSelection)) return false;
      const message = error?.code === 'timeout'
        ? '操作超时，请检查 AstrBot 状态后重试'
        : String(error?.message || '更正未完成，请稍后重试');
      notice(message, 'error');
      $('#apiState').textContent = `异常：${message}`;
      return false;
    }
  }

  async function handlePersonaRollback(target) {
    const requestSelection = selectionFromElement(target);
    const versionId = Number(target?.dataset?.rollback);
    if (!selectionIsCurrent(requestSelection) || !requestSelection.identityToken ||
        !Number.isInteger(versionId) || versionId <= 0) return false;
    if (!(await confirmAction('确定回滚到此人格版本吗？回滚会启动该维度的冷却期。'))) return false;
    if (!selectionIsCurrent(requestSelection)) return false;
    try {
      await post(ENDPOINTS.rollback, {
        identity_token: requestSelection.identityToken,
        version_id: versionId,
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection)) return false;
      openGrowthVersionId = null;
      growthEvidencePage = 1;
      renderVersionDetail(null, requestSelection);
      await load(requestSelection);
      return true;
    } catch (error) {
      if (!selectionIsCurrent(requestSelection)) return false;
      reportError(error);
      return false;
    }
  }

  async function rememberPreference() {
    const requestSelection = selectionSnapshot();
    const targetIdentityToken = requestSelection.identityToken;
    const targetPartitionToken = requestSelection.partitionToken;
    const typePath = String($('#memoryRememberType')?.value || '').trim();
    const contentField = $('#memoryRememberContent');
    const content = String(contentField?.value || '').trim();
    if (!targetIdentityToken && !targetPartitionToken) {
      notice('请先选择操作对象', 'error');
      return;
    }
    if (!content) {
      notice('请输入要记住的偏好内容', 'error');
      syncRememberForm();
      return;
    }
    const confirmed = await confirmAction('这会直接写入当前选定身份的 Atomic Memory，不会修改原始聊天，也不会自动修改 Persona。确定记住吗？');
    if (!confirmed || !selectionIsCurrent(requestSelection)) return;
    try {
      const result = await post(ENDPOINTS.memoryRemember, {
        identity_token: targetIdentityToken,
        partition_token: targetPartitionToken,
        type_path: typePath,
        content,
      }, requestSelection);
      if (!selectionIsCurrent(requestSelection)) return;
      clearRememberForm();
      if (!await loadMemoryPage(memoryPage, requestSelection)) return;
      if (!await openMemoryDetail(result.atom_id, requestSelection)) return;
      if (!await refreshMemoryProjections(requestSelection)) return;
      if (!selectionIsCurrent(requestSelection)) return;
      notice('手动偏好已写入当前 Atomic Memory');
    } catch (error) {
      if (!selectionIsCurrent(requestSelection)) return;
      if (error?.code === 'api_error' && error?.message === 'atomic_manual_remember_duplicate') {
        notice('这条当前记忆已经存在。', 'error');
        return;
      }
      reportError(error);
    }
  }

  async function loadPartitions(expectedSelection = selectionSnapshot()) {
    const request = beginSelectionRequest('portrait-directory', expectedSelection);
    if (!request) return false;
    const root = $('#portraitDirectory');
    const list = root?.querySelector('.portrait-directory-list');
    if (list) list.innerHTML = core.stateMarkup('loading', '正在读取当前身份目录……');
    try {
      const payload = await get(ENDPOINTS.partitions, { identity_token: request.identityToken });
      if (!selectionRequestIsCurrent(request)) return false;
      renderPortraitDirectory(payload?.items || [], request);
      return true;
    } catch (error) {
      if (!selectionRequestIsCurrent(request)) return false;
      const currentList = root?.querySelector('.portrait-directory-list');
      if (currentList) currentList.innerHTML = core.stateMarkup('error', '画像目录读取失败', true)
        .replace('data-retry="all"', `data-retry="all" data-selection-generation="${text(request.generation)}"`);
      return false;
    }
  }

  const PAGE_ORDER = ['overview', 'memory', 'profile', 'persona', 'cognition', 'versions', 'settings'];
  function navigateToPage(page, { updateHash = true } = {}) {
    const target = PAGE_ORDER.includes(page) ? page : 'overview';
    const previous = document.querySelector('[data-page].is-active')?.dataset.page || 'overview';
    const direction = PAGE_ORDER.indexOf(target) >= PAGE_ORDER.indexOf(previous) ? 'right' : 'left';
    document.querySelectorAll('[data-page]').forEach(pane => {
      const active = pane.dataset.page === target;
      pane.classList.toggle('is-active', active);
      pane.classList.toggle('page-slide-right', active && direction === 'right');
      pane.classList.toggle('page-slide-left', active && direction === 'left');
      pane.setAttribute('aria-hidden', active ? 'false' : 'true');
    });
    document.querySelectorAll('[data-page-nav]').forEach(link => {
      const active = link.dataset.pageNav === target;
      link.classList.toggle('active', active);
      link.setAttribute('aria-current', active ? 'page' : 'false');
    });
    if (updateHash && window.location.hash !== `#${target}`) window.history.replaceState(null, '', `#${target}`);
  }

  function pageFromHash() {
    const value = String(window.location.hash || '').replace(/^#/, '');
    return PAGE_ORDER.includes(value) ? value : 'overview';
  }

  function bind() {
    $('#plasticity').oninput = event => { $('#plasticityValue').value = event.target.value; };
    $('#identity').onchange = event => {
      setSelection(event.target.value, '');
      clearMaintenancePreview('已切换操作对象，请重新生成 Preview。');
      clearMaintenanceHistory();
      clearRememberForm();
      try {
        sessionStorage.setItem('lumielle_identity_token', selectedIdentityToken);
      } catch (e) {
        // sandbox iframe 下无法持久化，忽略
      }
      // 记住选择到后端，iframe 重载后可由后端回退恢复
      const requestSelection = selectionSnapshot();
      post(ENDPOINTS.remember, { identity_token: requestSelection.identityToken }, requestSelection).catch(() => {});
      load(requestSelection).then(() => {
        if (memoryView === 'maintenance') loadMemoryMaintenanceHistory();
      });
    };
    document.querySelectorAll('[data-page-nav]').forEach(link => {
      link.onclick = event => {
        event.preventDefault();
        navigateToPage(link.dataset.pageNav || 'overview');
      };
    });
    window.addEventListener('hashchange', () => navigateToPage(pageFromHash(), { updateHash: false }));
    navigateToPage(pageFromHash(), { updateHash: false });
    $('#refresh').onclick = () => load();
    $('#memoryApply').onclick = () => loadMemoryPage(1);
    document.querySelectorAll('[data-memory-view]').forEach(button => {
      button.onclick = () => {
        if (button.dataset.memoryView === 'atomic') {
          setMemoryView('atomic').catch(error => reportError(error, '#atomicStructureList'));
        } else if (button.dataset.memoryView === 'maintenance') {
          setMemoryView('maintenance').catch(error => reportError(error, '#memoryMaintenancePreview'));
        } else {
          setMemoryView('list').catch(error => reportError(error, '#memoryList'));
        }
      };
    });
    $('#memoryMaintenanceGenerate').onclick = () => {
      loadMemoryMaintenancePreview().catch(error => reportError(error, '#memoryMaintenancePreview'));
    };
    $('#memoryMaintenanceApply').onclick = () => {
      applyMemoryMaintenance().catch(error => reportError(error, '#memoryMaintenancePreview'));
    };
    $('#memoryMaintenanceHistoryRefresh').onclick = () => {
      loadMemoryMaintenanceHistory().catch(error => reportError(error, '#memoryMaintenanceHistory'));
    };
    $('#memoryMaintenanceHistory').onclick = event => {
      const button = event.target.closest('[data-consolidation-rollback]');
      if (button && !selectionIsCurrent(selectionFromElement(button))) return;
      if (button) rollbackMemoryConsolidation(button.dataset.consolidationRollback);
      const conflictRollback = event.target.closest('[data-conflict-rollback]');
      if (conflictRollback && !selectionIsCurrent(selectionFromElement(conflictRollback))) return;
      if (conflictRollback) rollbackMemoryConflict(conflictRollback.dataset.conflictRollback);
      const qualityRollback = event.target.closest('[data-quality-rollback]');
      if (qualityRollback && !selectionIsCurrent(selectionFromElement(qualityRollback))) return;
      if (qualityRollback) rollbackMemoryQuality(qualityRollback.dataset.qualityRollback);
      const conflictAtom = event.target.closest('[data-conflict-atom]');
      if (conflictAtom && !selectionIsCurrent(selectionFromElement(conflictAtom))) return;
      if (conflictAtom) openConflictAtom(conflictAtom.dataset.conflictAtom);
    };
    $('#memoryMaintenanceWarnings').onclick = event => {
      const button = event.target.closest('[data-maintenance-review-atom]');
      if (!button) return;
      const selection = selectionFromElement(button);
      if (!selectionIsCurrent(selection)) return;
      const atomId = Number(button.dataset.maintenanceReviewAtom);
      if (!Number.isInteger(atomId) || atomId <= 0) return;
      return openMemoryDetail(atomId, selection);
    };
    $('#memoryRememberContent').addEventListener('input', syncRememberForm);
    $('#memoryRememberSubmit').onclick = () => {
      return rememberPreference().catch(error => reportError(error));
    };
    $('#memoryPager').onclick = event => {
      const button = event.target.closest('[data-memory-page]') || event.target;
      const selection = selectionFromElement(button);
      if (!selectionIsCurrent(selection)) return;
      const action = button.dataset.memoryPage;
      if (action === 'prev') loadMemoryPage(memoryPage - 1, selection);
      if (action === 'next') loadMemoryPage(memoryPage + 1, selection);
    };
    $('#memoryList').onclick = event => {
      const button = event.target.closest('[data-memory-id]');
      if (!button) return;
      openMemoryDetail(button.dataset.memoryId, selectionFromElement(button));
    };
    $('#atomicStructure').onclick = event => {
      const listButton = event.target.closest('[data-structure-subject-list]');
      if (listButton) {
        const selection = selectionFromElement(listButton);
        if (!selectionIsCurrent(selection)) return;
        $('#memorySubject').value = listButton.dataset.structureSubjectList || '';
        setMemoryView('list').catch(error => reportError(error, '#memoryList'));
        loadMemoryPage(1, selection);
        return;
      }
      const button = event.target.closest('[data-atomic-node]');
      if (button) openMemoryDetail(button.dataset.atomicNode, selectionFromElement(button));
    };
    $('#atomicStructurePager').onclick = event => {
      const button = event.target.closest('[data-structure-page]') || event.target;
      const selection = selectionFromElement(button);
      if (!selectionIsCurrent(selection)) return;
      const action = button.dataset.structurePage;
      if (action === 'prev') loadMemoryStructure(structurePage - 1);
      if (action === 'next') loadMemoryStructure(structurePage + 1);
    };
    $('#portraitDirectory').onclick = event => {
      const card = event.target.closest('[data-partition-token]');
      if (!card || !selectionIsCurrent(selectionFromElement(card))) return;
      setSelection('', card.dataset.partitionToken || '');
      clearMaintenancePreview('已切换 UMO，请重新生成 Preview。');
      clearMaintenanceHistory();
      clearRememberForm();
      loadMemoryPage(1);
      loadProfile();
      loadOverview();
      if (memoryView === 'maintenance') loadMemoryMaintenanceHistory();
      navigateToPage('profile');
    };
    $('#profileContent').onclick = event => {
      const accordion = event.target.closest('[data-accordion]');
      if (accordion) {
        document.querySelectorAll('#profileContent details[data-profile-group]').forEach(detail => {
          detail.open = accordion.dataset.accordion === 'expand';
        });
        return;
      }
      const loadMore = event.target.closest('[data-profile-section-load]');
      if (loadMore) {
        const selection = selectionFromElement(loadMore);
        if (!selectionIsCurrent(selection)) return;
        const key = loadMore.dataset.profileSectionLoad;
        const group = loadMore.closest('details')?.querySelector('.profile-group-items');
        loadProfileSection(group, key, selection).catch(error => reportError(error, '#profileContent'));
        return;
      }
      const button = event.target.closest('[data-memory-id]');
      if (!button) return;
      openMemoryDetail(button.dataset.memoryId, selectionFromElement(button));
    };
    $('#memoryDetail').onclick = event => {
      const target = event.target.closest('button') || event.target;
      const selection = selectionFromElement(target, selectionFromElement($('#memoryDetail')));
      if (!selectionIsCurrent(selection)) return;
      if (event.target.closest('[data-close-memory]')) { $('#memoryDetail').hidden = true; $('#memoryDetail').classList.remove('is-open'); return; }
      const forgetButton = event.target.closest('[data-forget-atom]');
      if (forgetButton) { forgetMemoryAtom(forgetButton.dataset.forgetAtom, selectionFromElement(forgetButton)); return; }
      const correctButton = event.target.closest('[data-correct-atom]');
      if (correctButton) { correctMemoryAtom(correctButton.dataset.correctAtom, selectionFromElement(correctButton)); return; }
      const sibling = event.target.closest('[data-memory-id]');
      if (sibling) openMemoryDetail(sibling.dataset.memoryId, selectionFromElement(sibling));
    };
    $('#memorySubject').addEventListener('change', () => loadMemoryPage(1));
    $('#profileContent').addEventListener('click', event => {
      const topic = event.target.closest('[data-profile-topic]');
      if (!topic) return;
      const selection = selectionFromElement(topic);
      if (!selectionIsCurrent(selection)) return;
      $('#memorySubject').value = topic.dataset.profileTopic || '';
      navigateToPage('memory');
      loadMemoryPage(1, selection);
    });
    $('#memoryCompositionStrip').addEventListener('click', event => {
      const chip = event.target.closest('[data-pipeline-filter]');
      if (!chip || chip.dataset.selectionGeneration === undefined || chip.dataset.selectionGeneration === '') return;
      const selection = selectionFromElement(chip);
      if (!selectionIsCurrent(selection)) return;
      $('#memoryType').value = `${chip.dataset.pipelineFilter}.`;
      navigateToPage('memory');
      loadMemoryPage(1, selection);
    });
    $('#save').onclick = async () => {
      const requestSelection = selectionSnapshot();
      const settingsScope = $('#settingsScope').value;
      if (settingsScope === 'identity' && !requestSelection.identityToken) {
        notice('保存用户覆盖前，请先选择操作对象', 'error');
        return;
      }
      try {
        await post(ENDPOINTS.settings, {
          identity_token: requestSelection.identityToken,
          settings_scope: settingsScope,
          scope_mode: document.querySelector('input[name=route]:checked').value,
          autonomy_profile: $('#mode').value,
          plasticity: Number($('#plasticity').value),
        }, requestSelection);
        if (!selectionIsCurrent(requestSelection)) return;
        notice('设置已保存');
        await load(requestSelection);
      } catch (error) {
        if (!selectionIsCurrent(requestSelection)) return;
        reportError(error);
      }
    };
    document.addEventListener('click', async event => {
      const target = event.target.closest?.('button') || event.target;
      if (target.dataset.growthPage) {
        const selection = selectionFromElement(target);
        if (!selectionIsCurrent(selection)) return;
        const next = target.dataset.growthPage === 'prev'
          ? Math.max(1, growthTimelinePage - 1) : growthTimelinePage + 1;
        await loadPersonaGrowth(next, selection);
        return;
      }
      if (target.dataset.versionDetail) {
        await loadPersonaVersionDetail(target.dataset.versionDetail, 1, selectionFromElement(target));
        return;
      }
      if (target.dataset.versionEvidencePage && openGrowthVersionId != null) {
        const selection = selectionFromElement(target);
        if (!selectionIsCurrent(selection)) return;
        const next = target.dataset.versionEvidencePage === 'prev'
          ? Math.max(1, growthEvidencePage - 1) : growthEvidencePage + 1;
        await loadPersonaVersionDetail(openGrowthVersionId, next, selection);
        return;
      }
      if (target.dataset.growthAtom) {
        const snapshot = selectionFromElement(target);
        if (!selectionIsCurrent(snapshot)) return;
        navigateToPage('memory');
        await openMemoryDetail(target.dataset.growthAtom, snapshot);
        return;
      }
      if (target.dataset.growthDetailClose) {
        if (!selectionIsCurrent(selectionFromElement(target))) return;
        renderVersionDetail(null);
        openGrowthVersionId = null;
        return;
      }
      const retry = event.target.closest?.('[data-retry]') || event.target;
      if (retry.dataset.retry) {
        const selection = selectionFromElement(retry);
        if (!selectionIsCurrent(selection)) return;
        await load(selection);
        return;
      }
      const action = target.dataset.action;
      if (action) {
        await handleReviewAction(target);
      }
      if (target.dataset.rollback) await handlePersonaRollback(target);
    });
  }

  if (window.__LUMIELLE_ECHO_TEST__) {
    window.__LumielleEchoTestHooks = {
      rememberPreference,
      renderReviews,
      reviewCandidate,
      handleReviewAction,
      renderMaintenanceHistory,
      renderMaintenancePreview,
      loadMemoryMaintenancePreview,
      applyMemoryMaintenance,
      rollbackMemoryConflict,
      rollbackMemoryQuality,
      openConflictAtom,
      renderCurrentPersona,
      renderGrowthTimeline,
      renderVersionDetail,
      loadPersonaGrowth,
      loadPersonaVersionDetail,
      load,
      loadProfile,
      loadProfileSection,
      loadOverview,
      loadMemoryPage,
      loadMemoryStructure,
      loadPartitions,
      openMemoryDetail,
      forgetMemoryAtom,
      correctMemoryAtom,
      handlePersonaRollback,
      renderProfile,
      renderMemoryList,
      renderAtomicStructure,
      openGrowthEvidenceAtom: async atomId => {
        const snapshot = selectionSnapshot();
        navigateToPage('memory');
        return openMemoryDetail(atomId, snapshot);
      },
      bind,
      renderVersions,
      setBridge: value => { bridge = value; },
      setSelection,
      selectionSnapshot,
    };
    return;
  }
  bind();
  try {
    bridge = await waitForBridge();
    await core.requestWithTimeout(() => bridge.ready(), 8000);
    $('#bridgeState').textContent = '连接正常';
    $('#connectionSummary').textContent = 'Bridge 已连接';
  } catch (error) {
    $('#bridgeState').textContent = '不可用';
    $('#connectionSummary').textContent = '连接失败';
    reportError(error, '#reviews');
    renderState('#versions', 'error', '请从 AstrBot 插件页面打开本面板。', true);
    return;
  }
  await load();
})();
