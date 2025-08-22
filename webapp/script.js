document.addEventListener('DOMContentLoaded', function () {
    const tg = window.Telegram.WebApp;
    tg.expand(); // Expand the web app to full height

    const loader = document.getElementById('loader');
    const mainContent = document.getElementById('main-content');

    // 1. Get Data from URL
    function getUrlData() {
        try {
            const urlParams = new URLSearchParams(window.location.search);
            const dataParam = urlParams.get('data');
            if (!dataParam) {
                console.error("No data found in URL.");
                displayError("Could not load performance data.");
                return null;
            }
            // Decode and parse the JSON data
            return JSON.parse(decodeURIComponent(dataParam));
        } catch (error) {
            console.error("Error parsing data from URL:", error);
            displayError("There was an error processing your data. It might be corrupted.");
            return null;
        }
    }

    // 2. Populate the UI with data
    function populateUI(data) {
        if (!data) return;

        // Populate Header
        document.getElementById('user-name').textContent = `For ${data.userName || 'You'}`;

        // Populate Overall Stats
        const stats = data.overallStats;
        document.getElementById('overall-accuracy').textContent = `${stats.overallAccuracy}%`;
        document.getElementById('total-questions').textContent = stats.totalQuestions;
        document.getElementById('best-subject').textContent = stats.bestSubject;
        
        // Populate Coach's Insight
        document.getElementById('insight-text').textContent = data.coachInsight;

        // Populate Performance by Topic
        const topicList = document.getElementById('topic-list');
        topicList.innerHTML = ''; // Clear any existing content

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
            topicList.innerHTML = '<p>No topic-wise performance data available yet. Keep playing!</p>';
        }

        // Hide loader and show content
        loader.classList.add('hidden');
        mainContent.classList.remove('hidden');
    }

    function displayError(message) {
        loader.innerHTML = `<p style="color: #e94560;">${message}</p>`;
    }

    // Main execution
    const performanceData = getUrlData();
    populateUI(performanceData);
});