'use strict';

document.addEventListener('DOMContentLoaded', function () {
    const tg = window.Telegram.WebApp;
    tg.expand(); // Web App ko poori height tak expand karega

    // Zaroori elements ko select karna
    const loader = document.getElementById('loader');
    const mainContent = document.getElementById('main-content');

    // Function 1: URL se data nikalna
    function getUrlData() {
        try {
            const urlParams = new URLSearchParams(window.location.search);
            const dataParam = urlParams.get('data');
            if (!dataParam) {
                console.error("URL mein data nahi mila.");
                displayError("Performance data load nahi ho saka.");
                return null;
            }
            // Data ko decode karke JSON mein badalna
            return JSON.parse(decodeURIComponent(dataParam));
        } catch (error) {
            console.error("URL se data parse karne mein error:", error);
            displayError("Aapka data process karne mein error aa gaya. Shayad data corrupt hai.");
            return null;
        }
    }

    // Function 2: UI ko data se bharna
    function populateUI(data) {
        if (!data) return;

        // Header mein user ka naam daalna
        document.getElementById('user-name').textContent = `For ${data.userName || 'You'}`;

        // Overall Stats bharna
        const stats = data.overallStats;
        document.getElementById('overall-accuracy').textContent = `${stats.overallAccuracy}%`;
        document.getElementById('total-questions').textContent = stats.totalQuestions;
        document.getElementById('best-subject').textContent = stats.bestSubject;
        
        // Coach's Insight bharna
        document.getElementById('insight-text').textContent = data.coachInsight;

        // Topic-wise performance bharna
        const topicList = document.getElementById('topic-list');
        topicList.innerHTML = ''; // Pehle se मौजूद content ko saaf karna

        if (data.performanceByTopic && data.performanceByTopic.length > 0) {
            data.performanceByTopic.forEach(topic => {
                const topicCard = document.createElement('div');
                topicCard.className = 'topic-card';

                let breakdownHtml = `
                    <table class="breakdown-table">
                        <tr>
                            <th>Type</th>
                            <th>Score</th>
                            <th>Accuracy</th>
                            <th>Avg. Speed</th>
                        </tr>
                `;
                topic.breakdown.forEach(item => {
                    breakdownHtml += `
                        <tr>
                            <td>${item.type}</td>
                            <td>${item.correct}/${item.total}</td>
                            <td>
                                <div class="accuracy-bar-bg">
                                    <div class="accuracy-bar" style="width: ${item.accuracy}%;"></div>
                                </div>
                            </td>
                            <td>${item.avgSpeed}s</td>
                        </tr>
                    `;
                });
                breakdownHtml += `</table>`;

                topicCard.innerHTML = `
                    <div class="topic-header">
                        <h4>${topic.topicName}</h4>
                        <span class="topic-accuracy">${topic.accuracy}%</span>
                    </div>
                    <div class="topic-summary">
                        <span>Total: <b>${topic.totalCorrect}/${topic.totalQuestions}</b></span>
                        <span>Avg. Speed: <b>${topic.avgSpeed}s</b></span>
                    </div>
                    ${breakdownHtml}
                `;
                topicList.appendChild(topicCard);
            });
        } else {
            topicList.innerHTML = '<p>Abhi tak topic-wise performance data nahi hai. Quizzes khelte rahein!</p>';
        }

        // Loader chhupana aur content dikhana
        loader.classList.add('hidden');
        mainContent.classList.remove('hidden');
    }

    function displayError(message) {
        loader.innerHTML = `<p style="color: #e94560;">${message}</p>`;
    }

    // Main script ka execution
    const performanceData = getUrlData();
    populateUI(performanceData);
});