'use strict';

document.addEventListener('DOMContentLoaded', function () {
    const tg = window.Telegram.WebApp;

    // Element references
    const welcomeScreen = document.getElementById('welcome-screen');
    const conversationalView = document.getElementById('conversational-view');
    const dashboardView = document.getElementById('dashboard-view');
    const startGuidedBtn = document.getElementById('start-guided-btn');
    const skipToDashboardBtn = document.getElementById('skip-to-dashboard-btn');
    const userNameSpan = document.getElementById('user-name');
    const tabs = document.querySelectorAll('.tab-link');
    const tabContents = document.querySelectorAll('.tab-content');

    // MOCK USER DATA for standalone testing
    let userData = {
        overallStats: { totalQuizzes: 42, overallAccuracy: 78, bestSubject: 'Adv. Accounts', currentStreak: 5 },
        deepDive: {
            subjects: [
                { name: 'Adv. Accounts', accuracy: 85, avgSpeed: 30.5 },
                { name: 'Law', accuracy: 72, avgSpeed: 25.1 },
                { name: 'Taxation', accuracy: 68, avgSpeed: 45.8 }
            ],
            questionTypes: { practical: 70, theory: 85 }
        },
        coachInsight: "Your performance in Law is consistent, but focus more on practical questions in Taxation."
    };

    // Try to get REAL DATA from URL
    try {
        const urlParams = new URLSearchParams(window.location.search);
        const encodedData = urlParams.get('data');
        if (encodedData) {
            const decodedData = decodeURIComponent(encodedData);
            userData = JSON.parse(decodedData);
        }
    } catch (error) {
        console.error("Could not parse user data from URL, using mock data:", error);
    }
    
    // Initialize Telegram Web App
    tg.ready();
    tg.expand(); // App ko poori screen par expand kar do

    // Set User Name
    if (tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.first_name) {
        userNameSpan.textContent = tg.initDataUnsafe.user.first_name;
    } else {
        userNameSpan.textContent = 'Hey Buddy';
    }

    // Event Listeners
    startGuidedBtn.addEventListener('click', () => showConversationalView());
    skipToDashboardBtn.addEventListener('click', () => showDashboardView());
    tabs.forEach(tab => {
        tab.addEventListener('click', () => switchTab(tab.getAttribute('data-tab')));
    });

    // --- Main Functions ---
    function showConversationalView() {
        welcomeScreen.classList.add('hidden');
        dashboardView.classList.add('hidden');
        conversationalView.classList.remove('hidden');
        conversationalView.innerHTML = `<div class="card"><h2>Let's Chat! 🤖</h2><p>Our conversational coach is currently preparing for exams. This feature will be available soon!</p></div>`;
    }

    function showDashboardView() {
        welcomeScreen.classList.add('hidden');
        conversationalView.classList.add('hidden');
        dashboardView.classList.remove('hidden');
        renderAllTabs(userData);
    }
    
    function switchTab(targetTabId) {
        tabs.forEach(t => t.classList.remove('active'));
        tabContents.forEach(tc => tc.classList.remove('active'));
        document.querySelector(`[data-tab='${targetTabId}']`).classList.add('active');
        document.getElementById(targetTabId).classList.add('active');
    }

    // --- Render Functions (The Magic Happens Here) ---
    function renderAllTabs(data) {
        renderDashboardTab(data.overallStats, data.deepDive.subjects);
        renderDeepDiveTab(data.deepDive);
        renderActionsTab();
    }

    function renderDashboardTab(stats, subjects) {
        const container = document.getElementById('overall-stats-container');
        container.innerHTML = `
            <div class="card-grid">
                <div class="stat-card"><h3>${stats.totalQuizzes || 0}</h3><p>Quizzes Played</p></div>
                <div class="stat-card"><h3>${stats.overallAccuracy || 0}%</h3><p>Avg. Accuracy</p></div>
                <div class="stat-card"><h3>🔥 ${stats.currentStreak || 0}</h3><p>Current Streak</p></div>
                <div class="stat-card"><h3>🏆 ${stats.bestSubject || 'N/A'}</h3><p>Best Subject</p></div>
            </div>
        `;
        
        let existingChart = Chart.getChart('radarChart');
        if (existingChart) existingChart.destroy();

        const ctx = document.getElementById('radarChart').getContext('2d');
        new Chart(ctx, {
            type: 'radar',
            data: {
                labels: subjects.map(s => s.name),
                datasets: [{
                    label: 'Accuracy %',
                    data: subjects.map(s => s.accuracy),
                    fill: true,
                    backgroundColor: 'rgba(88, 166, 255, 0.2)',
                    borderColor: 'rgb(88, 166, 255)',
                    pointBackgroundColor: 'rgb(88, 166, 255)',
                    pointBorderColor: '#fff',
                    pointHoverBackgroundColor: '#fff',
                    pointHoverBorderColor: 'rgb(88, 166, 255)'
                }]
            },
            options: {
                scales: { r: { angleLines: { color: '#30363d' }, suggestedMin: 0, suggestedMax: 100, grid: { color: '#30363d' }, pointLabels: { color: '#c9d1d9' }, ticks: { color: '#8b949e', backdropColor: 'transparent' } } },
                plugins: { legend: { display: false } }
            }
        });
    }

    function renderDeepDiveTab(data) {
        const tableContainer = document.getElementById('subjects-table-container');
        let tableHTML = `
            <div class="table-wrapper">
                <table>
                    <thead><tr><th>Subject</th><th>Accuracy</th><th>Avg. Speed (s)</th></tr></thead>
                    <tbody>`;
        data.subjects.forEach(s => {
            tableHTML += `<tr><td>${s.name}</td><td>${s.accuracy}%</td><td>${s.avgSpeed}s</td></tr>`;
        });
        tableHTML += `</tbody></table></div>`;
        tableContainer.innerHTML = tableHTML;

        let existingChart = Chart.getChart('doughnutChart');
        if (existingChart) existingChart.destroy();

        const ctx = document.getElementById('doughnutChart').getContext('2d');
        new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Practical', 'Theory'],
                datasets: [{
                    data: [data.questionTypes.practical, data.questionTypes.theory],
                    backgroundColor: ['#58a6ff', '#388bfd'],
                    borderColor: 'var(--card-color)'
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { position: 'top', labels: { color: '#c9d1d9' } } }
            }
        });
    }

    function renderActionsTab() {
        const container = document.getElementById('quick-actions-container');
        container.innerHTML = `
            <p>Use these quick actions to interact with the bot directly in the group chat.</p>
            <button id="today-quiz-btn">📅 Show Today's Schedule</button>
            <button id="list-files-btn">🗂️ Browse All Notes</button>
            <button id="group-link-btn">🚀 Join the Main Group</button>
        `;
        
        document.getElementById('today-quiz-btn').addEventListener('click', () => {
            tg.sendData("/todayquiz"); // Bot ko command bhejega
            tg.close();
        });
        document.getElementById('list-files-btn').addEventListener('click', () => {
            tg.sendData("/listfile");
            tg.close();
        });
        document.getElementById('group-link-btn').addEventListener('click', () => {
            tg.openLink('https://t.me/cainterquizhub'); // Group ka link
        });
    }
});