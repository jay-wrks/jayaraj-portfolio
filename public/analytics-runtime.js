(function () {
  if (typeof window === "undefined" || typeof document === "undefined") return;
  if (typeof output === "undefined" || typeof input === "undefined" || typeof form === "undefined") return;

  const config = window.PORTFOLIO_ANALYTICS_CONFIG || {};
  const apiBase = String(config.apiBase || "").replace(/\/+$/, "");
  const ingestUrl = String(config.ingestUrl || (apiBase ? `${apiBase}/ingest` : "")).trim();
  const analyticsEnabled = Boolean(ingestUrl);
  const siteName = config.siteName || document.location.hostname || "portfolio";
  const ANALYTICS_VERSION = "1.0";
  const STORAGE_VISITOR_KEY = "portfolio.analytics.visitor";
  const SESSION_IDLE_WINDOW_MS = 30000;
  const MAX_PREVIEW_CHARS = 280;
  const IDLE_FLUSH_DELAY_MS = 20000;
  const MAX_QUEUE_SIZE = 10;

  const queue = [];
  const sectionObserverTargets = new WeakSet();
  const scrollMilestones = new Set();
  const typingState = {
    active: false,
    startedAt: 0,
    lastInputAt: 0,
    keyCount: 0,
    backspaceCount: 0,
    source: "keyboard"
  };

  let pendingTypingMetrics = null;
  let flushTimer = 0;
  let flushInFlight = false;
  const createdExecutions = [];
  let lastAccountingAt = Date.now();
  let scrollSampleTimer = 0;

  const visitorId = getOrCreateVisitorId();
  const sessionId = createId("sess");
  const pageId = createId("page");
  const sessionState = {
    startedAt: Date.now(),
    visibleMs: 0,
    activeMs: 0,
    commandCount: 0,
    clickCount: 0,
    lastInteractionAt: Date.now(),
    maxScrollDepth: 0
  };

  const sectionObserver = "IntersectionObserver" in window
    ? new IntersectionObserver(entries => {
        entries.forEach(entry => {
          if (!entry.isIntersecting || entry.intersectionRatio < 0.4) return;
          const view = entry.target?.dataset?.analyticsView;
          if (!view) return;
          queueEvent("section_view", {
            section: view,
            label: view,
            commandName: entry.target.dataset.analyticsCommand || "",
            commandSource: entry.target.dataset.analyticsSource || ""
          });
          sectionObserver.unobserve(entry.target);
        });
      }, { root: output, threshold: [0.4, 0.7] })
    : null;

  function createId(prefix) {
    if (window.crypto?.randomUUID) return `${prefix}_${window.crypto.randomUUID()}`;
    return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
  }

  function getOrCreateVisitorId() {
    try {
      const existing = localStorage.getItem(STORAGE_VISITOR_KEY);
      if (existing) return existing;
      const created = createId("visitor");
      localStorage.setItem(STORAGE_VISITOR_KEY, created);
      return created;
    } catch {
      return createId("visitor");
    }
  }

  function trackInteraction() {
    sessionState.lastInteractionAt = Date.now();
    scheduleFlush();
  }

  function accountSessionTime() {
    const now = Date.now();
    const delta = Math.max(0, now - lastAccountingAt);
    if (!document.hidden) {
      sessionState.visibleMs += delta;
      if (now - sessionState.lastInteractionAt <= SESSION_IDLE_WINDOW_MS) {
        sessionState.activeMs += delta;
      }
    }
    lastAccountingAt = now;
    sessionState.maxScrollDepth = Math.max(sessionState.maxScrollDepth, getScrollDepth());
  }

  function getScrollDepth() {
    if (!output) return 0;
    const maxScrollable = Math.max(1, output.scrollHeight - output.clientHeight);
    if (maxScrollable <= 1) return 1;
    return Number(Math.min(1, Math.max(0, output.scrollTop / maxScrollable)).toFixed(4));
  }

  function getDeviceType() {
    if (typeof mobileQuery !== "undefined" && mobileQuery.matches) return "mobile";
    const width = Math.min(window.innerWidth || 0, window.screen?.width || Infinity);
    if (width && width < 720) return "mobile";
    if (width && width < 1100) return "tablet";
    return "desktop";
  }

  function truncate(value, size = MAX_PREVIEW_CHARS) {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    if (text.length <= size) return text;
    return `${text.slice(0, size - 1)}…`;
  }

  function compactObject(object) {
    return Object.fromEntries(
      Object.entries(object).filter(([, value]) =>
        value !== undefined &&
        value !== null &&
        value !== "" &&
        !(typeof value === "number" && Number.isNaN(value))
      )
    );
  }

  function buildEvent(eventType, payload = {}) {
    accountSessionTime();
    const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || "";
    return compactObject({
      siteName,
      eventType,
      eventId: createId("evt"),
      sessionId,
      visitorId,
      pageId,
      occurredAt: new Date().toISOString(),
      path: location.pathname,
      href: location.href,
      title: document.title,
      referrer: document.referrer || "",
      language: navigator.language || "",
      timezone: timeZone,
      platform: navigator.platform || "",
      userAgent: navigator.userAgent || "",
      deviceType: getDeviceType(),
      viewportW: Math.round(window.innerWidth || 0),
      viewportH: Math.round(window.innerHeight || 0),
      screenW: Math.round(window.screen?.width || 0),
      screenH: Math.round(window.screen?.height || 0),
      pixelRatio: Number((window.devicePixelRatio || 1).toFixed(2)),
      touchPoints: navigator.maxTouchPoints || 0,
      hardwareConcurrency: navigator.hardwareConcurrency || 0,
      sessionDurationMs: Math.max(0, Date.now() - sessionState.startedAt),
      visibleMs: Math.round(sessionState.visibleMs),
      activeMs: Math.round(sessionState.activeMs),
      commandCount: sessionState.commandCount,
      clickCount: sessionState.clickCount,
      scrollDepth: getScrollDepth(),
      maxScrollDepth: sessionState.maxScrollDepth,
      ...payload
    });
  }

  function scheduleFlush() {
    if (!analyticsEnabled || flushTimer) return;
    flushTimer = window.setTimeout(() => {
      flushTimer = 0;
      const idleForMs = Date.now() - sessionState.lastInteractionAt;
      if (queue.length > 0 && queue.length < MAX_QUEUE_SIZE && idleForMs >= IDLE_FLUSH_DELAY_MS) {
        flushQueue();
        return;
      }
      if (queue.length > 0 && queue.length < MAX_QUEUE_SIZE) {
        scheduleFlush();
      }
    }, IDLE_FLUSH_DELAY_MS);
  }

  function queueEvent(eventType, payload = {}, options = {}) {
    if (!analyticsEnabled) return;
    const event = buildEvent(eventType, payload);
    console.log(`[portfolio-analytics v${ANALYTICS_VERSION}] event`, event);
    queue.push(event);
    if (options.immediate || queue.length >= MAX_QUEUE_SIZE) {
      flushQueue(options);
      return;
    }
    scheduleFlush();
  }

  function flushQueue(options = {}) {
    if (!analyticsEnabled || !queue.length || (flushInFlight && !options.beacon)) return;
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = 0;
    }

    const batch = queue.splice(0, queue.length);
    const body = JSON.stringify({ events: batch });
    if (options.beacon && navigator.sendBeacon) {
      const ok = navigator.sendBeacon(ingestUrl, new Blob([body], { type: "application/json" }));
      if (ok) return;
    }

    flushInFlight = true;
    fetch(ingestUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
      keepalive: options.keepalive !== false,
      credentials: "omit"
    }).catch(() => {
      queue.unshift(...batch);
    }).finally(() => {
      flushInFlight = false;
      if (queue.length) scheduleFlush();
    });
  }

  function startTyping(source) {
    if (!typingState.active) {
      typingState.active = true;
      typingState.startedAt = Date.now();
      typingState.keyCount = 0;
      typingState.backspaceCount = 0;
    }
    typingState.source = source || typingState.source;
    typingState.lastInputAt = Date.now();
  }

  function finalizeTyping(command) {
    if (!typingState.active) return null;
    const endedAt = Date.now();
    const text = String(command || "").trim();
    const metrics = {
      typingDurationMs: Math.max(0, (typingState.lastInputAt || endedAt) - typingState.startedAt),
      keyCount: typingState.keyCount,
      backspaceCount: typingState.backspaceCount,
      commandLength: text.length,
      typingSource: typingState.source
    };
    typingState.active = false;
    typingState.startedAt = 0;
    typingState.lastInputAt = 0;
    typingState.keyCount = 0;
    typingState.backspaceCount = 0;
    typingState.source = "keyboard";
    return metrics;
  }

  function consumeTypingMetrics(command, source) {
    const next = pendingTypingMetrics;
    pendingTypingMetrics = null;
    if (next) return next;
    if (source !== "user") return null;
    return {
      typingDurationMs: 0,
      keyCount: 0,
      backspaceCount: 0,
      commandLength: String(command || "").trim().length,
      typingSource: "shortcut"
    };
  }

  function summarizeExecutionOutput(execution) {
    if (!execution?.nodes) return { outputChars: 0, outputLines: 0, outputPreview: "" };
    const text = [...execution.nodes]
      .map(node => {
        if (!(node instanceof Node)) return "";
        if (node instanceof HTMLElement) return node.innerText || node.textContent || "";
        return node.textContent || "";
      })
      .join("\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim();
    return {
      outputChars: text.length,
      outputLines: text ? text.split(/\r?\n/).filter(Boolean).length : 0,
      outputPreview: truncate(text)
    };
  }

  function viewLabelForNode(node) {
    if (!(node instanceof HTMLElement)) return "";
    if (node.matches(".section")) {
      return node.querySelector(".section-title")?.textContent?.trim() || "Section";
    }
    if (node.matches(".intro-shell")) return "Overview";
    if (node.matches(".manager-brief")) return "Impact Brief";
    if (node.matches(".packet-inspector-shell")) return "Packet Inspector";
    return "";
  }

  function observeAnalyticsViews(nodes, execution) {
    nodes.forEach(node => {
      if (!(node instanceof HTMLElement)) return;
      [node, ...node.querySelectorAll(".section,.intro-shell,.manager-brief,.packet-inspector-shell")]
        .forEach(candidate => {
          if (sectionObserverTargets.has(candidate)) return;
          const label = viewLabelForNode(candidate);
          if (!label) return;
          sectionObserverTargets.add(candidate);
          candidate.dataset.analyticsView = label;
          candidate.dataset.analyticsCommand = execution?.analytics?.label || "";
          candidate.dataset.analyticsSource = execution?.analytics?.source || "";
          if (sectionObserver) sectionObserver.observe(candidate);
          else queueEvent("section_view", { section: label, label });
        });
    });
  }

  function trackScrollMilestones() {
    const depth = getScrollDepth();
    [0.25, 0.5, 0.75, 1].forEach(mark => {
      if (depth >= mark && !scrollMilestones.has(mark)) {
        scrollMilestones.add(mark);
        queueEvent("scroll_depth", { label: `${Math.round(mark * 100)}%`, scrollDepth: depth });
      }
    });
  }

  const originalCreateCommandExecution = createCommandExecution;
  createCommandExecution = function patchedCreateCommandExecution(label) {
    const execution = originalCreateCommandExecution(label);
    execution.analytics = {
      label,
      source: "user",
      rawCommand: "",
      startedAt: performance.now(),
      hadError: false
    };
    createdExecutions.push(execution);
    return execution;
  };

  const originalAppendHTML = appendHTML;
  appendHTML = function patchedAppendHTML(html, animated, execution) {
    const trackedExecution = execution || (typeof currentCommandExecution !== "undefined" ? currentCommandExecution : null);
    if (trackedExecution?.analytics && /\bline\s+error\b/.test(String(html))) {
      trackedExecution.analytics.hadError = true;
    }
    const nodes = originalAppendHTML(html, animated, trackedExecution);
    observeAnalyticsViews(nodes, trackedExecution);
    return nodes;
  };

  const originalAppendLine = appendLine;
  appendLine = function patchedAppendLine(text, className, animated, execution) {
    const trackedExecution = execution || (typeof currentCommandExecution !== "undefined" ? currentCommandExecution : null);
    if (trackedExecution?.analytics && String(className || "").includes("error")) {
      trackedExecution.analytics.hadError = true;
    }
    return originalAppendLine(text, className, animated, trackedExecution);
  };

  const originalRunCommand = runCommand;
  runCommand = async function patchedRunCommand(raw, options = {}) {
    const rawCommand = String(raw || "");
    const trimmedCommand = rawCommand.trim();
    const source = options.source || (options.showPrompt === false ? "system" : "user");
    const startedAt = performance.now();
    const tokens = typeof parseShellInput === "function" ? parseShellInput(trimmedCommand) : trimmedCommand.split(/\s+/).filter(Boolean);
    const firstToken = (tokens[0] || "").toLowerCase();
    const normalized = trimmedCommand.replace(/\s+/g, " ").toLowerCase();
    const typingMetrics = consumeTypingMetrics(trimmedCommand, source);
    const startingExecutionId = typeof commandExecutionId === "number" ? commandExecutionId : 0;

    try {
      return await originalRunCommand(raw, options);
    } finally {
      if (!trimmedCommand) return;
      sessionState.commandCount += 1;

      const createdIndex = createdExecutions.findIndex(execution => execution?.id > startingExecutionId);
      const execution = createdIndex >= 0 ? createdExecutions.splice(createdIndex, 1)[0] : null;
      if (execution?.analytics) {
        execution.analytics.rawCommand = trimmedCommand;
        execution.analytics.source = source;
      }

      const outputSummary = summarizeExecutionOutput(execution);
      const status = execution
        ? (execution.cancelled ? "interrupted" : (execution.analytics?.hadError ? "error" : "success"))
        : "not_found";

      queueEvent("command_completed", compactObject({
        command: trimmedCommand,
        commandName: normalized || firstToken || trimmedCommand.toLowerCase(),
        commandFamily: firstToken || normalized,
        commandSource: source,
        status,
        commandDurationMs: Math.round(performance.now() - startedAt),
        ...typingMetrics,
        ...outputSummary
      }));
    }
  };

  output.addEventListener("scroll", () => {
    trackInteraction();
    if (scrollSampleTimer) return;
    scrollSampleTimer = window.setTimeout(() => {
      scrollSampleTimer = 0;
      accountSessionTime();
      trackScrollMilestones();
    }, 220);
  }, { passive: true });

  document.addEventListener("visibilitychange", () => {
    accountSessionTime();
    if (document.hidden) flushQueue({ beacon: true });
  });

  document.addEventListener("click", event => {
    trackInteraction();
    const actionable = event.target.closest("a,button,[data-command]");
    sessionState.clickCount += 1;
    queueEvent("click", compactObject({
      xNorm: Number((event.clientX / Math.max(1, window.innerWidth)).toFixed(4)),
      yNorm: Number((event.clientY / Math.max(1, window.innerHeight)).toFixed(4)),
      targetTag: actionable?.tagName || event.target?.tagName || "",
      targetText: truncate(actionable?.textContent || event.target?.textContent || "", 120),
      linkTarget: actionable?.href || "",
      label: actionable?.dataset?.command || ""
    }));
  }, { passive: true });

  document.addEventListener("keydown", () => {
    trackInteraction();
  });

  input.addEventListener("input", () => {
    trackInteraction();
    if (input.value) startTyping("keyboard");
  });

  input.addEventListener("keydown", event => {
    trackInteraction();
    if (event.key.length === 1 || event.key === "Backspace" || event.key === "Delete" || event.key === "Tab" || event.key === "Enter") {
      startTyping("keyboard");
    }
    if (event.key.length === 1) typingState.keyCount += 1;
    if (event.key === "Backspace" || event.key === "Delete") typingState.backspaceCount += 1;
  });

  form.addEventListener("submit", () => {
    trackInteraction();
    pendingTypingMetrics = finalizeTyping(input.value);
  }, true);

  input.addEventListener("blur", () => {
    if (!input.value.trim() && typingState.active) {
      pendingTypingMetrics = finalizeTyping("");
    }
  });

  console.log(`[portfolio-analytics v${ANALYTICS_VERSION}] initialized`);
  queueEvent("page_view", { label: document.title || location.pathname });

  window.addEventListener("pagehide", () => {
    accountSessionTime();
    queueEvent("session_end", {
      label: "session_end",
      scrollDepth: getScrollDepth(),
      maxScrollDepth: sessionState.maxScrollDepth
    });
    flushQueue({ beacon: true });
  });
})();
