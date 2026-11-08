(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  root.LumielleEchoPageCore = api;
}(typeof globalThis === 'object' ? globalThis : this, function () {
  class PageRequestError extends Error {
    constructor(code, message) {
      super(message);
      this.name = 'PageRequestError';
      this.code = code;
    }
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function requireBridge(bridge) {
    if (!bridge || typeof bridge.apiGet !== 'function' ||
        typeof bridge.apiPost !== 'function') {
      throw new PageRequestError(
        'bridge_missing', '未检测到 AstrBot 插件页面 Bridge'
      );
    }
    return bridge;
  }

  function unwrap(value) {
    return value && value.data !== undefined ? value.data : value;
  }

  function normalize(value) {
    const payload = unwrap(value);
    if (payload && payload.ok === false) {
      throw new PageRequestError(
        'api_error', String(payload.error || 'AstrBot API 返回错误')
      );
    }
    return payload;
  }

  function requestWithTimeout(operation, timeoutMs = 8000) {
    const timeout = Math.max(1, Number(timeoutMs) || 8000);
    return new Promise((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        reject(new PageRequestError(
          'timeout', `请求超过 ${timeout} 毫秒仍未完成`
        ));
      }, timeout);
      Promise.resolve()
        .then(operation)
        .then(normalize)
        .then(value => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          resolve(value);
        })
        .catch(error => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          reject(error instanceof PageRequestError
            ? error
            : new PageRequestError(
              'request_failed', String(error?.message || error || '请求失败')
            ));
        });
    });
  }

  function stateMarkup(state, message, retry = false) {
    const safeState = ['loading', 'empty', 'error'].includes(state)
      ? state : 'empty';
    const action = retry
      ? '<button type="button" class="state-retry" data-retry="all">重试</button>'
      : '';
    return `<div class="view-state view-state--${safeState}" data-state="${safeState}">
      <span class="state-dot" aria-hidden="true"></span>
      <p>${escapeHtml(message)}</p>${action}
    </div>`;
  }

  return {
    PageRequestError,
    escapeHtml,
    requireBridge,
    requestWithTimeout,
    stateMarkup,
  };
}));
