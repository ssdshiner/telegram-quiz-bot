'use strict';

document.addEventListener('DOMContentLoaded', function () {
    const tg = window.Telegram.WebApp;
    tg.expand();

    const loader = document.getElementById('loader');
    const appContainer = document.getElementById('app-container');

    // Function to get data from URL
    function getUrlData() {
        try {
            const dataParam = new URLSearchParams(window.location.search).get('data');
            if (!dataParam) throw new Error("URL mein data nahi mila.");
            return JSON.parse(decodeURIComponent(dataParam));
        } catch (error) {
            console.error("Data parse karne mein error:", error);
            loader.innerHTML = `<p style="color: #f85149;">Performance data load nahi ho saka. Please try again.</p>`;
            return null;
        }
    }

    const userData = getUrlData();

    if (userData) {
        if (userData.isDataAvailable) {
            initializeDashboard(userData);
        } else {
            loader.innerHTML = `<p>${userData.coachInsight}</p>`;
        }
    }

    function initializeDashboard(data) {
        // Populate header
        document.getElementById('user-name').textContent = data.userName;
        document.getElementById('coach-insight').textContent = data.coachInsight;

        // Render all components
        renderOverallStats(data.overallStats);
        renderRadarChart(data.charts.radar);
        renderDeepDiveTable(data.deepDive.subjects);
        renderDoughnutChart(data.charts.doughnut);
        renderQuickActions();

        // Setup tab functionality
        setupTabs();

        // Show the app
        loader.classList.add('hidden');
        appContainer.classList.remove('hidden');
    }

    function renderOverallStats(stats) {
        const container = document.getElementById('overall-stats-container');
        container.innerHTML = `
            <div class="stat-item">
                <span class="stat-value">${stats.overallAccuracy}%</span>
                <span class="stat-label">Overall Accuracy</span>
            </div>
            <div class="stat-item">
                <span class="stat-value">${stats.totalQuestions}</span>
                <span class="stat-label">Total Questions</span>
            </div>
            <div class="stat-item">
                <span class="stat-value">${stats.bestSubject}</span>
                <span class="stat-label">Best Subject</span>
            </div>
        `;
    }

    function renderRadarChart(chartData) {
        const ctx = document.getElementById('radarChart').getContext('2d');
        new Chart(ctx, {
            type: 'radar',
            data: {
                labels: chartData.labels,
                datasets: [{
                    label: 'Accuracy %',
                    data: chartData.data,
                    backgroundColor: 'rgba(88, 166, 255, 0.2)',
                    borderColor: 'rgba(88, 166, 255, 1)',
                    borderWidth: 2
                }]
            },
            options: {
                scales: { r: { angleLines: { color: '#30363d' }, grid: { color: '#30363d' }, pointLabels: { color: '#c9d1d9' }, suggestedMin: 0, suggestedMax: 100 } },
                plugins: { legend: { display: false } }
            }
        });
    }

    function renderDeepDiveTable(subjects) {
        const container = document.getElementById('subjects-table-container');
        let tableHtml = '<table class="subjects-table"><tr><th>Subject</th><th>Accuracy</th><th>Score</th></tr>';
        subjects.forEach(s => {
            let accuracyClass = 'accuracy-mid';
            if (s.accuracy >= 80) accuracyClass = 'accuracy-high';
            if (s.accuracy < 50) accuracyClass = 'accuracy-low';
            tableHtml += `<tr><td>${s.name}</td><td class="accuracy-cell ${accuracyClass}">${s.accuracy}%</td><td>${s.score}</td></tr>`;
        });
        tableHtml += '</table>';
        container.innerHTML = tableHtml;
    }

    function renderDoughnutChart(chartData) {
        const ctx = document.getElementById('doughnutChart').getContext('2d');
        new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: chartData.labels,
                datasets: [{
                    data: chartData.data,
                    backgroundColor: ['#58a6ff', '#3fb950', '#f2c97d'],
                    borderColor: 'var(--card-color)',
                    borderWidth: 3
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { position: 'top', labels: { color: '#c9d1d9' } } }
            }
        });
    }
    
    function renderQuickActions() {
        const container = document.getElementById('quick-actions-container');
        container.innerHTML = `
            <button class="action-btn" data-command="/todayquiz">📅 Show Today's Schedule</button>
            <button class="action-btn" data-command="/listfile">🗂️ Browse All Notes</button>
            <button class="action-btn" data-command="/mystats">📊 Get My Quick Stats</button>
        `;
        container.querySelectorAll('.action-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                tg.sendData(btn.dataset.command);
                tg.close();
            });
        });
    }

    function setupTabs() {
        const tabs = document.querySelectorAll('.tab-link');
        const tabContents = document.querySelectorAll('.tab-content');
        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                tabs.forEach(item => item.classList.remove('active'));
                tab.classList.add('active');
                const target = document.getElementById(tab.dataset.tab);
                tabContents.forEach(content => content.classList.remove('active'));
                target.classList.add('active');
            });
        });
    }
});