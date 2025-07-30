'use strict';

document.addEventListener('DOMContentLoaded', function () {
    const tg = window.Telegram.WebApp;

    // ... (saare element variables same rahenge)
    const welcomeScreen = document.getElementById('welcome-screen');
    const conversationalView = document.getElementById('conversational-view');
    const dashboardView = document.getElementById('dashboard-view');
    const startGuidedBtn = document.getElementById('start-guided-btn');
    const skipToDashboardBtn = document.getElementById('skip-to-dashboard-btn');
    const userNameSpan = document.getElementById('user-name');
    const tabs = document.querySelectorAll('.tab-link');
    const tabContents = document.querySelectorAll('.tab-content');

    // === NEW LOGIC: REAL DATA from URL ===
    // Hum URL se data nikalne ki koshish karenge
    let userData = null;
    try {
        const urlParams = new URLSearchParams(window.location.search);
        const encodedData = urlParams.get('data');
        if (encodedData) {
            const decodedData = decodeURIComponent(encodedData);
            userData = JSON.parse(decodedData);
        }
    } catch (error) {
        console.error("Could not parse user data from URL:", error);
    }
    
    // Agar URL se data nahi mila, to testing ke liye MOCK data use karo
    if (!userData) {
        userData = {
            overallStats: { overallAccuracy: 0, bestSubject: 'N/A' },
            deepDive: { subjects: [{ name: 'Test Subject', accuracy: 50 }] }
        };
    }

    // Telegram Integration & Initialization
    tg.ready();
    if (tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.first_name) {
        userNameSpan.textContent = tg.initDataUnsafe.user.first_name;
    } else {
        userNameSpan.textContent = 'Hey Buddy';
    }

    // Event Listeners (No Change)
    startGuidedBtn.addEventListener('click', () => showConversationalView());
    skipToDashboardBtn.addEventListener('click', () => showDashboardView());
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const targetTab = tab.getAttribute('data-tab');
            tabs.forEach(t => t.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));
            tab.classList.add('active');
            document.getElementById(targetTab).classList.add('active');
        });
    });

    // Main Functions (Updated to use real data)
    function showConversationalView() {
        // ... (same as before)
        welcomeScreen.classList.add('hidden');
        dashboardView.classList.add('hidden');
        conversationalView.classList.remove('hidden');
        conversationalView.innerHTML = `<div class="card"><h2>Let's Chat!</h2><p>Our conversational coach is under construction...</p></div>`;
    }

    function showDashboardView() {
        // ... (same as before)
        welcomeScreen.classList.add('hidden');
        conversationalView.classList.add('hidden');
        dashboardView.classList.remove('hidden');
        renderDashboardData(userData); // Pass the real or mock userData
    }

    function renderDashboardData(data) {
        const dashboardTab = document.getElementById('dashboard-tab');
        
        let existingChart = Chart.getChart('accuracyChart');
        if (existingChart) {
            existingChart.destroy();
        }

        // Add overall stats to the dashboard tab
        dashboardTab.innerHTML = `
            <div class="card">
                <h2>Overall Stats</h2>
                <p>Average Accuracy: ${data.overallStats.overallAccuracy}%</p>
                <p>Best Subject: ${data.overallStats.bestSubject}</p>
            </div>
            <div class="card">
                <h2>Subject-wise Accuracy</h2>
                <canvas id="accuracyChart"></canvas>
            </div>
        `;
        
        const ctx = document.getElementById('accuracyChart').getContext('2d');
        const labels = data.deepDive.subjects.map(subject => subject.name);
        const accuracyData = data.deepDive.subjects.map(subject => subject.accuracy);

        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Accuracy %',
                    data: accuracyData,
                    backgroundColor: 'rgba(88, 166, 255, 0.6)',
                    borderColor: 'rgba(88, 166, 255, 1)',
                    borderWidth: 1
                }]
            },
            options: { /* ... (options same as before) ... */ }
        });
    }
});