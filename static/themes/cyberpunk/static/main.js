let currentEventSource = null;
let historyVisible = false; // Track history panel status

function createTask() {
    const promptInput = document.getElementById('prompt-input');
    const prompt = promptInput.value.trim();

    if (!prompt) {
        alert("Please enter a valid task");
        promptInput.focus();
        return;
    }

    if (currentEventSource) {
        currentEventSource.close();
        currentEventSource = null;
    }

    const taskContainer = document.getElementById('task-container');
    const stepsContainer = document.getElementById('steps-container');
    const resultContainer = document.getElementById('result-container');
    const container = document.querySelector('.container');
    const resultPanel = document.getElementById('result-panel');

    // 重置UI状态
    resetUIState();

    // Hide welcome message, show step loading status
    const welcomeMessage = taskContainer.querySelector('.welcome-message');
    if (welcomeMessage) {
        welcomeMessage.style.display = 'none';
    }

    stepsContainer.innerHTML = '<div class="loading">Initializing task...</div>';
    resultContainer.innerHTML = '';

    // Close history panel on mobile devices
    closeHistoryOnMobile();

    fetch('/tasks', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ prompt })
    })
    .then(response => {
        if (!response.ok) {
            return response.json().then(err => { throw new Error(err.detail || 'Request failed') });
        }
        return response.json();
    })
    .then(data => {
        if (!data.task_id) {
            throw new Error('Invalid task ID');
        }
        setupSSE(data.task_id);
        loadHistory();
        promptInput.value = '';
    })
    .catch(error => {
        stepsContainer.innerHTML = `<div class="error">Error: ${error.message}</div>`;
        updateResultPanel({result: error.message}, 'error');
        showResultPanel();
        console.error('Failed to create task:', error);
    });
}

// 重置UI状态的独立函数，避免重复代码
function resetUIState() {
    const container = document.querySelector('.container');
    const resultPanel = document.getElementById('result-panel');

    // 重置容器布局
    container.classList.remove('with-result');
    container.style.width = '98%';

    // 确保结果面板完全隐藏
    if (resultPanel) {
        resultPanel.classList.add('hidden');
        resultPanel.style.display = 'none';
    }

    // 触发布局调整
    handleResponsiveLayout();
}

function setupSSE(taskId) {
    let retryCount = 0;
    const maxRetries = 3;
    const retryDelay = 2000;
    let lastResultContent = '';
    let stepsData = [];

    const stepsContainer = document.getElementById('steps-container');
    const resultContainer = document.getElementById('result-container');

    // Hide result panel by default
    hideResultPanel();

    function connect() {
        const eventSource = new EventSource(`/tasks/${taskId}/events`);
        currentEventSource = eventSource;

        let heartbeatTimer = setInterval(() => {
            const pingDiv = document.createElement('div');
            pingDiv.className = 'ping';
            pingDiv.innerHTML = '·';
            stepsContainer.appendChild(pingDiv);
        }, 5000);

        // Initial polling
        fetch(`/tasks/${taskId}`)
            .then(response => response.json())
            .then(task => {
                updateTaskStatus(task);
            })
            .catch(error => {
                console.error('Initial status retrieval failed:', error);
            });

        const handleEvent = (event, type) => {
            clearInterval(heartbeatTimer);
            try {
                const data = JSON.parse(event.data);
                const loadingDiv = stepsContainer.querySelector('.loading');
                if (loadingDiv) loadingDiv.remove();

                const { formattedContent, timestamp, isoTimestamp } = formatStepContent(data, type);

                stepsData.push({
                    type: type,
                    content: formattedContent,
                    timestamp: timestamp,
                    isoTimestamp: isoTimestamp,
                    element: createStepElement(type, formattedContent, timestamp)
                });

                stepsData.sort((a, b) => {
                    return new Date(a.isoTimestamp) - new Date(b.isoTimestamp);
                });

                stepsContainer.innerHTML = '';
                stepsData.forEach(step => {
                    stepsContainer.appendChild(step.element);
                });

                document.querySelectorAll('.step-item').forEach(item => {
                    item.classList.remove('active');
                });

                const latestStep = stepsData[stepsData.length - 1];
                if (latestStep && latestStep.element) {
                    latestStep.element.classList.add('active');
                }

                autoScroll(stepsContainer);

                if (type === 'tool' || type === 'act' || type === 'result') {
                    updateResultPanel(data, type);
                    showResultPanel();
                }

                fetch(`/tasks/${taskId}`)
                    .then(response => response.json())
                    .then(task => {
                        updateTaskStatus(task);
                    })
                    .catch(error => {
                        console.error('Failed to update status:', error);
                    });
            } catch (e) {
                console.error(`Error processing ${type} event:`, e);
            }
        };

        const eventTypes = ['think', 'tool', 'act', 'log', 'run', 'message'];
        eventTypes.forEach(type => {
            eventSource.addEventListener(type, (event) => handleEvent(event, type));
        });

        eventSource.addEventListener('complete', (event) => {
            clearInterval(heartbeatTimer);
            try {
                const data = JSON.parse(event.data);
                lastResultContent = data.result || '';

                const completeDiv = document.createElement('div');
                completeDiv.className = 'complete';
                completeDiv.innerHTML = '<div>✅ Task completed</div>';
                stepsContainer.appendChild(completeDiv);

                updateResultPanel({result: lastResultContent}, 'complete');
                showResultPanel();

                fetch(`/tasks/${taskId}`)
                    .then(response => response.json())
                    .then(task => {
                        updateTaskStatus(task);
                    })
                    .catch(error => {
                        console.error('Failed to update final status:', error);
                    });

                eventSource.close();
                currentEventSource = null;
            } catch (e) {
                console.error('Error processing completion event:', e);
            }
        });

        eventSource.addEventListener('error', (event) => {
            clearInterval(heartbeatTimer);
            try {
                const data = JSON.parse(event.data);
                const errorDiv = document.createElement('div');
                errorDiv.className = 'error';
                errorDiv.innerHTML = `<div>❌ Error: ${data.message}</div>`;
                stepsContainer.appendChild(errorDiv);

                updateResultPanel({result: data.message}, 'error');
                showResultPanel();

                eventSource.close();
                currentEventSource = null;
            } catch (e) {
                console.error('Error processing error:', e);
            }
        });

        eventSource.onerror = (err) => {
            if (eventSource.readyState === EventSource.CLOSED) return;

            console.error('SSE connection error:', err);
            clearInterval(heartbeatTimer);
            eventSource.close();

            fetch(`/tasks/${taskId}`)
                .then(response => response.json())
                .then(task => {
                    if (task.status === 'completed' || task.status === 'failed') {
                        updateTaskStatus(task);
                        if (task.status === 'completed') {
                            const completeDiv = document.createElement('div');
                            completeDiv.className = 'complete';
                            completeDiv.innerHTML = '<div>✅ Task completed</div>';
                            stepsContainer.appendChild(completeDiv);

                            if (task.steps && task.steps.length > 0) {
                                const lastStep = task.steps[task.steps.length - 1];
                                updateResultPanel({result: lastStep.result}, 'complete');
                                showResultPanel();
                            }
                        } else {
                            const errorDiv = document.createElement('div');
                            errorDiv.className = 'error';
                            errorDiv.innerHTML = `<div>❌ Error: ${task.error || 'Task failed'}</div>`;
                            stepsContainer.appendChild(errorDiv);

                            updateResultPanel({result: task.error || 'Task failed'}, 'error');
                            showResultPanel();
                        }
                    } else if (retryCount < maxRetries) {
                        retryCount++;
                        const warningDiv = document.createElement('div');
                        warningDiv.className = 'warning';
                        warningDiv.innerHTML = `<div>⚠ Connection lost, retrying in ${retryDelay/1000} seconds (${retryCount}/${maxRetries})...</div>`;
                        stepsContainer.appendChild(warningDiv);
                        setTimeout(connect, retryDelay);
                    } else {
                        const errorDiv = document.createElement('div');
                        errorDiv.className = 'error';
                        errorDiv.innerHTML = '<div>⚠ Connection lost, please refresh the page</div>';
                        stepsContainer.appendChild(errorDiv);

                        updateResultPanel({result: 'Connection lost, please refresh the page'}, 'error');
                        showResultPanel();
                    }
                })
                .catch(error => {
                    console.error('Failed to check task status:', error);
                    if (retryCount < maxRetries) {
                        retryCount++;
                        setTimeout(connect, retryDelay);
                    }
                });
        };
    }

    connect();
}

function updateResultPanel(data, type) {
    const resultContainer = document.getElementById('result-container');
    const currentStep = document.getElementById('current-step');

    if (!resultContainer || !currentStep) return;

    // Update top step information
    const typeLabel = getEventLabel(type);
    const icon = getEventIcon(type);

    // Clear and build new UI
    currentStep.innerHTML = '';
    currentStep.setAttribute('data-type', type); // 添加类型属性

    // Add icon
    const iconSpan = document.createElement('span');
    iconSpan.className = 'emoji-icon';
    iconSpan.innerHTML = icon; // 使用innerHTML而不是textContent以支持HTML标签
    currentStep.appendChild(iconSpan);

    // Create status text element, add typewriter effect
    const statusText = document.createElement('span');
    statusText.className = 'status-text';
    currentStep.appendChild(statusText);

    // Typewriter effect displaying status text
    let i = 0;
    let typingEffect = setInterval(() => {
        if (i < typeLabel.length) {
            statusText.textContent += typeLabel.charAt(i);
            i++;
        } else {
            clearInterval(typingEffect);
        }
    }, 50);

    // Update content area
    let content = '';

    if (data.result) {
        content = data.result;
    } else if (data.message) {
        content = data.message;
    } else {
        content = JSON.stringify(data, null, 2);
    }

    // Clear previous content, add new content
    resultContainer.innerHTML = '';

    // Create content area with proper styling for overflow control
    const contentDiv = document.createElement('div');
    contentDiv.classList.add('content-box');
    contentDiv.style.width = '100%';
    contentDiv.style.maxWidth = '100%';
    contentDiv.style.boxSizing = 'border-box';
    contentDiv.style.overflowWrap = 'break-word';
    contentDiv.style.wordWrap = 'break-word';
    contentDiv.style.wordBreak = 'break-word';

    const preElement = document.createElement('pre');
    preElement.style.whiteSpace = 'pre-wrap';
    preElement.style.wordWrap = 'break-word';
    preElement.style.wordBreak = 'break-word';
    preElement.style.maxWidth = '100%';
    preElement.style.boxSizing = 'border-box';
    preElement.textContent = content;

    contentDiv.appendChild(preElement);
    resultContainer.appendChild(contentDiv);

    // Ensure proper scrolling
    setTimeout(() => {
        resultContainer.scrollTop = 0;
    }, 100);
}

function loadHistory() {
    fetch('/tasks')
    .then(response => {
        if (!response.ok) {
            return response.text().then(text => {
                throw new Error(`Request failed: ${response.status} - ${text.substring(0, 100)}`);
            });
        }
        return response.json();
    })
    .then(tasks => {
        const listContainer = document.getElementById('task-list');
        if (tasks.length === 0) {
            listContainer.innerHTML = '<div class="info">No history tasks</div>';
            return;
        }

        // Update history count
        const historyCount = document.querySelector('.history-count');
        if (historyCount) {
            historyCount.textContent = tasks.length;
        }

        listContainer.innerHTML = tasks.map(task => `
            <div class="task-card" data-task-id="${task.id}" onclick="loadTask('${task.id}')">
                <div class="task-title">${task.prompt}</div>
                <div class="task-meta">
                    <span>${new Date(task.created_at).toLocaleString()}</span>
                    <span class="status status-${task.status ? task.status.toLowerCase() : 'unknown'}">
                        ${getStatusText(task.status)}
                    </span>
                </div>
            </div>
        `).join('');
    })
    .catch(error => {
        console.error('Failed to load history:', error);
        const listContainer = document.getElementById('task-list');
        listContainer.innerHTML = `<div class="error">Loading failed: ${error.message}</div>`;
    });
}

function loadTask(taskId) {
    if (currentEventSource) {
        currentEventSource.close();
        currentEventSource = null;
    }

    const taskContainer = document.getElementById('task-container');
    const stepsContainer = document.getElementById('steps-container');
    const resultContainer = document.getElementById('result-container');

    // 重置UI状态
    resetUIState();

    // Hide welcome message
    const welcomeMessage = taskContainer.querySelector('.welcome-message');
    if (welcomeMessage) {
        welcomeMessage.style.display = 'none';
    }

    stepsContainer.innerHTML = '<div class="loading">Loading task...</div>';
    resultContainer.innerHTML = '';

    // Close history panel on mobile devices
    closeHistoryOnMobile();

    fetch(`/tasks/${taskId}`)
        .then(response => response.json())
        .then(task => {
            // 高亮当前选中的任务卡片
            highlightTaskCard(taskId);

            stepsContainer.innerHTML = '';
            if (task.steps && task.steps.length > 0) {
                // 按时间戳排序渲染步骤
                renderSortedSteps(task.steps, task.created_at);
            } else {
                stepsContainer.innerHTML = '<div class="info">No steps recorded for this task</div>';
            }

            updateTaskStatus(task);
        })
        .catch(error => {
            console.error('Failed to load task:', error);
            stepsContainer.innerHTML = `<div class="error">Error: ${error.message}</div>`;
        });
}

// 高亮当前选中的任务卡片
function highlightTaskCard(taskId) {
    const taskCards = document.querySelectorAll('.task-card');
    taskCards.forEach(card => {
        card.classList.remove('active');
        if (card.getAttribute('data-task-id') === taskId) {
            card.classList.add('active');
        }
    });
}

// 按时间戳排序渲染步骤
function renderSortedSteps(steps, taskCreatedAt) {
    const stepsContainer = document.getElementById('steps-container');

    // 存储步骤集合
    let taskSteps = [];

    steps.forEach((step, index) => {
        const stepTimestamp = new Date(step.created_at || taskCreatedAt).toLocaleTimeString();
        const stepElement = createStepElement(
            step.type,
            step.result,
            stepTimestamp
        );

        // 将步骤添加到集合
        taskSteps.push({
            index: index,
            timestamp: stepTimestamp,
            isoTimestamp: step.created_at || taskCreatedAt, // 保存ISO时间戳
            element: stepElement,
            step: step
        });
    });

    // 根据时间戳排序步骤
    taskSteps.sort((a, b) => {
        // 尝试使用ISO时间戳进行比较
        if (a.isoTimestamp && b.isoTimestamp) {
            return new Date(a.isoTimestamp) - new Date(b.isoTimestamp);
        }

        // 首先按时间戳排序，如果时间相同，按索引排序
        const timeCompare = new Date(a.timestamp) - new Date(b.timestamp);
        return timeCompare !== 0 ? timeCompare : a.index - b.index;
    });

    // 将排序后的步骤添加到容器
    taskSteps.forEach((stepData, index) => {
        // 只将最后一个步骤设为展开状态
        if (index === taskSteps.length - 1) {
            stepData.element.classList.add('expanded');
            stepData.element.classList.add('active');

            // 显示最后一个步骤的结果
            updateResultPanel({result: stepData.step.result}, stepData.step.type);
            showResultPanel();
        }

        stepsContainer.appendChild(stepData.element);
    });
}

function formatStepContent(data, eventType) {
    // 创建具有ISO格式的时间戳，确保排序一致性
    const now = new Date();
    const isoTimestamp = now.toISOString();
    const localTime = now.toLocaleTimeString();

    return {
        formattedContent: data.result || (data.message || JSON.stringify(data)),
        timestamp: localTime,
        isoTimestamp: isoTimestamp // 添加ISO格式时间戳，用于排序
    };
}

function createStepElement(type, content, timestamp) {
    const step = document.createElement('div');

    // Executing step
    const stepRegex = /Executing step (\d+)\/(\d+)/;
    if (type === 'log' && stepRegex.test(content)) {
        const match = content.match(stepRegex);
        const currentStep = parseInt(match[1]);
        const totalSteps = parseInt(match[2]);

        step.className = 'step-divider';
        step.innerHTML = `
            <div class="step-circle">${currentStep}</div>
            <div class="step-line"></div>
            <div class="step-info">${currentStep}/${totalSteps}</div>
        `;
    } else if (type === 'act') {
        // Check if it contains information about file saving
        const saveRegex = /Content successfully saved to (.+)/;
        const match = content.match(saveRegex);

        step.className = `step-item ${type}`;
        step.dataset.type = type;
        step.dataset.timestamp = timestamp; // 存储时间戳为数据属性

        // 获取图标HTML
        const iconHtml = getEventIcon(type);

        if (match && match[1]) {
            const filePath = match[1].trim();
            const fileName = filePath.split('/').pop();
            const fileExtension = fileName.split('.').pop().toLowerCase();

            // Handling different types of files
            let fileInteractionHtml = '';

            if (['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp'].includes(fileExtension)) {
                fileInteractionHtml = `
                    <div class="file-interaction image-preview">
                        <img src="${filePath}" alt="${fileName}" class="preview-image" onclick="showFullImage('${filePath}')">
                        <a href="/download?file_path=${filePath}" download="${fileName}" class="download-link">⬇️ 下载图片</a>
                    </div>
                `;
            } else if (['mp3', 'wav', 'ogg'].includes(fileExtension)) {
                fileInteractionHtml = `
                    <div class="file-interaction audio-player">
                        <audio controls src="${filePath}"></audio>
                        <a href="/download?file_path=${filePath}" download="${fileName}" class="download-link">⬇️ 下载音频</a>
                    </div>
                `;
            } else if (['html', 'js', 'py'].includes(fileExtension)) {
                fileInteractionHtml = `
                    <div class="file-interaction code-file">
                        <button onclick="simulateRunPython('${filePath}')" class="run-button">▶️ 模拟运行</button>
                        <a href="/download?file_path=${filePath}" download="${fileName}" class="download-link">⬇️ 下载文件</a>
                    </div>
                `;
            } else {
                fileInteractionHtml = `
                    <div class="file-interaction">
                        <a href="/download?file_path=${filePath}" download="${fileName}" class="download-link">⬇️ 下载文件: ${fileName}</a>
                    </div>
                `;
            }

            step.innerHTML = `
                <div class="log-header" onclick="toggleStepContent(this)">
                    <div class="log-prefix">
                        <span class="log-prefix-icon">${iconHtml}</span>
                        <span>${getEventLabel(type)}</span>
                        <time>${timestamp}</time>
                    </div>
                    <div class="content-preview">${content.substring(0, 20) + (content.length > 20 ? "..." : "")}</div>
                    <div class="step-controls">
                        <span class="minimize-btn" onclick="minimizeStep(event, this)"></span>
                    </div>
                </div>
                <div class="log-body">
                    <div class="log-content">
                        <pre>${content}</pre>
                        ${fileInteractionHtml}
                    </div>
                </div>
            `;
        } else {
            step.innerHTML = `
                <div class="log-header" onclick="toggleStepContent(this)">
                    <div class="log-prefix">
                        <span class="log-prefix-icon">${iconHtml}</span>
                        <span>${getEventLabel(type)}</span>
                        <time>${timestamp}</time>
                    </div>
                    <div class="content-preview">${content.substring(0, 20) + (content.length > 20 ? "..." : "")}</div>
                    <div class="step-controls">
                        <span class="minimize-btn" onclick="minimizeStep(event, this)"></span>
                    </div>
                </div>
                <div class="log-body">
                    <div class="log-content">
                        <pre>${content}</pre>
                    </div>
                </div>
            `;
        }
    } else {
        // Get content preview
        let contentPreview = "";
        if (type === 'think' && content.length > 0) {
            // Extract the first 30 characters of the thinking content as preview
            contentPreview = content.substring(0, 30) + (content.length > 30 ? "..." : "");
        } else if (type === 'tool' && content.includes('selected')) {
            // Tool selection content remains as is
            contentPreview = content;
        } else if (type === 'log') {
            // Log content remains as is, usually short
            contentPreview = content;
        } else {
            // Other types take the first 20 characters
            contentPreview = content.substring(0, 20) + (content.length > 20 ? "..." : "");
        }

        step.className = `step-item ${type}`;
        step.dataset.type = type;
        step.dataset.timestamp = timestamp; // 存储时间戳为数据属性

        // 获取图标HTML
        const iconHtml = getEventIcon(type);

        // 确保时间戳显示在log-prefix中，并将步骤类型标签包装在span标签中
        step.innerHTML = `
            <div class="log-header" onclick="toggleStepContent(this)">
                <div class="log-prefix">
                    <span class="log-prefix-icon">${iconHtml}</span>
                    <span>${getEventLabel(type)}</span>
                    <time>${timestamp}</time>
                </div>
                <div class="content-preview">${contentPreview}</div>
                <div class="step-controls">
                    <span class="minimize-btn" onclick="minimizeStep(event, this)"></span>
                </div>
            </div>
            <div class="log-body">
                <div class="log-content">
                    <pre>${content}</pre>
                </div>
            </div>
        `;
    }

    return step;
}

// Toggle display/hide of step content
function toggleStepContent(header) {
    const stepItem = header.closest('.step-item');
    if (!stepItem) return;

    const logBody = stepItem.querySelector('.log-body');
    if (!logBody) return;

    // 关闭其他展开的步骤
    document.querySelectorAll('.step-item.expanded').forEach(item => {
        if (item !== stepItem) {
            item.classList.remove('expanded');
        }
    });

    // 切换当前步骤的展开状态
    stepItem.classList.toggle('expanded');

    // 高亮当前步骤
    highlightStep(stepItem);

    // 如果步骤展开，更新结果面板并显示
    if (stepItem.classList.contains('expanded')) {
        const type = stepItem.dataset.type;
        const content = stepItem.querySelector('pre')?.textContent || '';

        updateResultPanel({result: content}, type);
        showResultPanel();
    }

    // 触发布局调整
    handleResponsiveLayout();
}

// Minimize step without toggling expansion
function minimizeStep(event, btn) {
    event.stopPropagation(); // 阻止事件冒泡

    const stepItem = btn.closest('.step-item');
    if (!stepItem) return;

    stepItem.classList.remove('expanded');

    // 触发布局调整
    handleResponsiveLayout();
}

// Toggle result panel display state
function toggleResultPanel() {
    const resultPanel = document.getElementById('result-panel');

    if (!resultPanel) return;

    if (resultPanel.classList.contains('hidden')) {
        showResultPanel();
    } else {
        hideResultPanel();
    }
}

function hideResultPanel() {
    const resultPanel = document.getElementById('result-panel');
    const container = document.querySelector('.container');

    if (!resultPanel) return;

    // 先添加hidden类，触发CSS动画
    resultPanel.classList.add('hidden');
    container.classList.remove('with-result');

    // 延迟处理样式变化，确保过渡效果
    setTimeout(function() {
        resultPanel.style.display = 'none';
        container.style.width = '98%';
        handleResponsiveLayout();
    }, 300);
}

function showResultPanel() {
    const resultPanel = document.getElementById('result-panel');
    const container = document.querySelector('.container');
    const resultContainer = document.getElementById('result-container');

    if (!resultPanel) return;

    // 设置为可见
    resultPanel.style.display = 'flex';
    resultPanel.style.flexDirection = 'column';

    // 确保结果容器可滚动且内容不溢出
    if (resultContainer) {
        resultContainer.style.overflowY = 'auto';
        resultContainer.style.overflowX = 'hidden';
        resultContainer.style.maxHeight = 'calc(100vh - 200px)';
        resultContainer.style.width = '100%';
        resultContainer.style.wordWrap = 'break-word';
        resultContainer.style.wordBreak = 'break-word';
        resultContainer.style.boxSizing = 'border-box';
        resultContainer.style.padding = '5px 10px 15px 5px';

        // 应用内容溢出处理
        ensureContentFitsContainer(resultContainer);
    }

    // 使用setTimeout确保DOM更新
    setTimeout(() => {
        resultPanel.classList.remove('hidden');
        container.classList.add('with-result');

        // 调整容器宽度
        if (window.innerWidth > 1024) {
            container.style.width = 'calc(68% - 10px)';
            // 确保结果面板的视觉效果正确显示
            resultPanel.style.transform = 'translateX(0)';
        }

        // 触发布局调整
        handleResponsiveLayout();

        // 再次应用内容溢出处理，确保所有内容都适应容器
        if (resultContainer) {
            ensureContentFitsContainer(resultContainer);
        }
    }, 50);
}

function autoScroll(element) {
    if (element) {
        element.scrollTop = element.scrollHeight;
    }
}

// 综合处理响应式布局的函数
function handleResponsiveLayout() {
    const container = document.querySelector('.container');
    const resultPanel = document.getElementById('result-panel');
    const stepsContainer = document.getElementById('steps-container');
    const resultContainer = document.getElementById('result-container');
    const isMobile = window.innerWidth <= 768;
    const isTablet = window.innerWidth <= 1024 && window.innerWidth > 768;

    // 确保滚动容器始终可滚动
    if (stepsContainer) {
        stepsContainer.style.overflowY = 'auto';
        stepsContainer.style.overflowX = 'hidden';
    }

    if (resultContainer) {
        resultContainer.style.overflowY = 'auto';
        resultContainer.style.overflowX = 'hidden';
        resultContainer.style.maxHeight = isMobile ? 'calc(50vh - 150px)' : 'calc(100vh - 200px)';
        resultContainer.style.width = '100%';
        resultContainer.style.boxSizing = 'border-box';

        // 确保内容不会溢出
        ensureContentFitsContainer(resultContainer);
    }

    // 调整步骤项布局
    adjustStepItemsLayout();

    // 根据屏幕尺寸调整容器宽度
    if (window.innerWidth <= 1024) {
        // 平板和手机布局
        container.style.width = '98%';
    } else {
        // 桌面布局
        if (resultPanel && !resultPanel.classList.contains('hidden')) {
            container.style.width = 'calc(68% - 10px)';
            container.classList.add('with-result');
        } else {
            container.style.width = '98%';
            container.classList.remove('with-result');
        }
    }

    // 根据屏幕尺寸确定历史面板显示
    if (historyVisible) {
        if (window.innerWidth > 768) {
            container.classList.add('with-history');
        } else {
            container.classList.remove('with-history');
        }
    }
}

// 确保内容适合容器
function ensureContentFitsContainer(container) {
    if (!container) return;

    // 查找容器内的所有内容元素
    const contentElements = container.querySelectorAll('p, div, pre, span, code, table');

    contentElements.forEach(element => {
        // 设置必要的样式以防止溢出
        element.style.maxWidth = '100%';
        element.style.wordWrap = 'break-word';
        element.style.wordBreak = 'break-word';
        element.style.overflowWrap = 'break-word';
        element.style.boxSizing = 'border-box';

        // 处理长字符串或代码
        if (element.scrollWidth > element.clientWidth) {
            element.style.whiteSpace = 'pre-wrap';
        }
    });
}

// 调整步骤项布局
function adjustStepItemsLayout() {
    const stepItems = document.querySelectorAll('.step-item');
    const isMobile = window.innerWidth <= 768;

    stepItems.forEach(item => {
        const logHeader = item.querySelector('.log-header');
        const contentPreview = item.querySelector('.content-preview');
        const logPrefix = item.querySelector('.log-prefix');
        const timeElement = logPrefix ? logPrefix.querySelector('time') : null;

        if (isMobile) {
            // 手机布局
            if (contentPreview) {
                contentPreview.style.maxWidth = 'calc(100% - 40px)';
                contentPreview.style.marginLeft = '34px';
            }

            if (timeElement) {
                timeElement.style.fontSize = '0.7rem';
            }
        } else {
            // 桌面布局
            if (contentPreview) {
                contentPreview.style.maxWidth = '';
                contentPreview.style.marginLeft = '';
            }

            if (timeElement) {
                timeElement.style.fontSize = '';
            }
        }
    });
}

function getEventIcon(type) {
    switch (type) {
        case 'think': return '<i class="fas fa-brain"></i>';
        case 'tool': return '<i class="fas fa-cog"></i>';
        case 'act': return '<i class="fas fa-wave-square"></i>';
        case 'log': return '<i class="fas fa-file-alt"></i>';
        case 'run': return '<i class="fas fa-play"></i>';
        case 'message': return '<i class="fas fa-comment"></i>';
        case 'complete': return '<i class="fas fa-check"></i>';
        case 'error': return '<i class="fas fa-times"></i>';
        default: return '<i class="fas fa-thumbtack"></i>';
    }
}

function getEventLabel(type) {
    switch (type) {
        case 'think': return 'Thinking';
        case 'tool': return 'Using Tool';
        case 'act': return 'Taking Action';
        case 'log': return 'Log';
        case 'run': return 'Running';
        case 'message': return 'Message';
        case 'complete': return 'Completed';
        case 'error': return 'Error';
        default: return 'Step';
    }
}

function updateTaskStatus(task) {
    const statusBar = document.getElementById('status-bar');
    if (!statusBar) return;

    if (task.status === 'completed') {
        statusBar.innerHTML = `<span class="status-complete">✅ Task completed</span>`;

        if (currentEventSource) {
            currentEventSource.close();
            currentEventSource = null;
        }
    } else if (task.status === 'failed') {
        statusBar.innerHTML = `<span class="status-error">❌ Task failed: ${task.error || 'Unknown error'}</span>`;

        if (currentEventSource) {
            currentEventSource.close();
            currentEventSource = null;
        }
    } else {
        statusBar.innerHTML = `<span class="status-running">⚙️ Task running: ${task.status}</span>`;
    }
}

function showFullImage(imageSrc) {
    let modal = document.getElementById('image-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'image-modal';
        modal.className = 'image-modal';
        modal.innerHTML = `
            <span class="close-modal">&times;</span>
            <img src="${imageSrc}" class="modal-content" id="full-image">
        `;
        document.body.appendChild(modal);

        const closeBtn = modal.querySelector('.close-modal');
        closeBtn.addEventListener('click', () => {
            modal.classList.remove('active');
        });

        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.classList.remove('active');
            }
        });
    } else {
        document.getElementById('full-image').src = imageSrc;
    }

    modal.classList.add('active');
}

function simulateRunPython(filePath) {
    let modal = document.getElementById('python-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'python-modal';
        modal.className = 'python-modal';
        modal.innerHTML = `
            <div class="python-console">
                <div class="close-modal">&times;</div>
                <div class="python-output">Loading Python file content...</div>
            </div>
        `;
        document.body.appendChild(modal);

        const closeBtn = modal.querySelector('.close-modal');
        closeBtn.addEventListener('click', () => {
            modal.classList.remove('active');
        });
    }

    modal.classList.add('active');

    // Load Python file content
    fetch(filePath)
        .then(response => response.text())
        .then(code => {
            const outputDiv = modal.querySelector('.python-output');
            outputDiv.innerHTML = '';

            const codeElement = document.createElement('pre');
            codeElement.textContent = code;
            codeElement.style.marginBottom = '20px';
            codeElement.style.padding = '10px';
            codeElement.style.borderBottom = '1px solid #444';
            outputDiv.appendChild(codeElement);

            // Add simulation run results
            const resultElement = document.createElement('div');
            resultElement.innerHTML = `
                <div style="color: #4CAF50; margin-top: 10px; margin-bottom: 10px;">
                    > Simulation run output results:</div>
                <pre style="color: #f8f8f8;">
# This is the simulation run output results
# Actual run results may vary

# Running ${filePath.split('/').pop()}...
print("Hello from Python Simulated environment!")

# Code execution completed
</pre>
            `;
            outputDiv.appendChild(resultElement);
        })
        .catch(error => {
            console.error('Failed to load Python file:', error);
            const outputDiv = modal.querySelector('.python-output');
            outputDiv.innerHTML = `File loading error: ${error.message}`;
        });
}

// Highlight current selected step
function highlightStep(stepElement) {
    // 移除其他步骤的高亮
    document.querySelectorAll('.step-item').forEach(item => {
        item.classList.remove('active');
    });

    // 添加当前步骤的高亮
    stepElement.classList.add('active');

    // 更新当前步骤信息到结果面板
    const currentStep = document.getElementById('current-step');
    if (currentStep) {
        const type = stepElement.dataset.type;
        currentStep.setAttribute('data-type', type);
    }
}

// Toggle history panel display state
function toggleHistory() {
    const historyPanel = document.querySelector('.history-panel');
    const overlay = document.querySelector('.overlay');
    const historyToggle = document.querySelector('.history-toggle');
    const container = document.querySelector('.container');

    if (historyVisible) {
        // Hide history
        historyPanel.classList.remove('show');
        overlay.classList.remove('show');
        historyToggle.classList.remove('active');
        container.classList.remove('with-history');
    } else {
        // Show history
        historyPanel.classList.add('show');
        overlay.classList.add('show');
        historyToggle.classList.add('active');
        // Add spacing on large screens
        if (window.innerWidth > 768) {
            container.classList.add('with-history');
        }
    }

    historyVisible = !historyVisible;
}

// Close history panel on small screens
function closeHistoryOnMobile() {
    if (window.innerWidth <= 768 && historyVisible) {
        toggleHistory();
    }
}

// Get status text
function getStatusText(status) {
    switch (status) {
        case 'pending': return 'Pending';
        case 'running': return 'Running';
        case 'completed': return 'Completed';
        case 'failed': return 'Failed';
        default: return 'Unknown';
    }
}

// Initialize interface
function initializeInterface() {
    // 添加历史记录切换逻辑
    const historyToggle = document.querySelector('.history-toggle');
    if (historyToggle) {
        historyToggle.addEventListener('click', toggleHistory);
    }

    // 添加遮罩层点击关闭历史记录
    const overlay = document.querySelector('.overlay');
    if (overlay) {
        overlay.addEventListener('click', toggleHistory);
    }

    // 绑定输入框事件
    const promptInput = document.getElementById('prompt-input');
    if (promptInput) {
        promptInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                createTask();
            }
        });
    }

    // 添加窗口大小调整事件监听
    window.addEventListener('resize', handleWindowResize);

    // 添加键盘事件监听，用于关闭模态框
    document.addEventListener('keydown', handleKeyboardEvents);

    // 添加屏幕方向变化监听
    window.addEventListener('orientationchange', () => {
        // 延迟执行以确保方向变化完成
        setTimeout(handleResponsiveLayout, 300);
    });

    // 阻止页面滚动
    preventPageScroll();

    // 设置初始布局
    setupInitialLayout();

    // 加载历史任务
    loadHistory();
}

// 阻止页面滚动
function preventPageScroll() {
    // 阻止文档滚轮事件，保留容器内滚动
    document.addEventListener('wheel', function(event) {
        // 检查是否在可滚动容器内
        const isInScrollable =
            event.target.closest('.steps-container') ||
            event.target.closest('.result-container') ||
            event.target.closest('.history-panel') ||
            event.target.closest('.log-body');

        if (!isInScrollable) {
            event.preventDefault();
        }
    }, { passive: false });
}

// 处理窗口大小调整
function handleWindowResize() {
    const container = document.querySelector('.container');

    // 大屏幕上保持历史侧边栏效果，小屏幕上移除
    if (window.innerWidth > 768 && historyVisible) {
        container.classList.add('with-history');
    } else {
        container.classList.remove('with-history');
    }

    // 调用综合处理函数
    handleResponsiveLayout();
}

// 处理键盘事件
function handleKeyboardEvents(e) {
    // ESC键关闭历史面板和模态框
    if (e.key === 'Escape') {
        if (historyVisible) {
            toggleHistory();
        }

        const imageModal = document.getElementById('image-modal');
        if (imageModal && imageModal.classList.contains('active')) {
            imageModal.classList.remove('active');
        }

        const pythonModal = document.getElementById('python-modal');
        if (pythonModal && pythonModal.classList.contains('active')) {
            pythonModal.classList.remove('active');
        }
    }
}

// 设置初始布局
function setupInitialLayout() {
    // 调整步骤项布局
    adjustStepItemsLayout();

    // 初始化历史面板状态
    const historyPanel = document.querySelector('.history-panel');
    if (historyPanel) {
        historyPanel.classList.remove('show');
    }

    // 确保结果面板初始隐藏
    const resultPanel = document.getElementById('result-panel');
    if (resultPanel) {
        hideResultPanel();
    }

    // 手动触发一次响应式布局
    handleResponsiveLayout();
}

// 当文档加载完成时初始化界面
document.addEventListener('DOMContentLoaded', initializeInterface);
