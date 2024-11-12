import { useState, useEffect } from 'react'
import './App.css'
import { PieChart } from '@mui/x-charts/PieChart';
import { List, ListItem } from '@mui/material';

function App() {
  const [sentiment, setSentiment] = useState([]);
  const [message, setMessage] = useState([]);
  function formatData(data) {
    return [
      { id: 0, value: data.total_positive ?? 0, label: 'Positive' },
      { id: 1, value: data.total_negative ?? 0, label: 'Negative' },
      { id: 2, value: data.total_neutral ?? 0, label: 'Neutral' },
    ];
  }

  useEffect(() => {
    const fetchData = () => {
      fetch('http://localhost:8000/stats/sentiment/summary')
        .then(response => {
          if (!response.ok) {
            throw new Error('Network response was not ok');
          }
          return response.json();
        })
        .then(data => {
          setSentiment(formatData(data));
        })
        .catch(error => {
          console.error('Error fetching data:', error);
        });

      fetch('http://localhost:8000/messages/sentiment')
        .then(response => {
          if (!response.ok) {
            throw new Error('Network response was not ok');
          }
          return response.json();
        })
        .then(data => {
          setMessage(data.messages);
        })
        .catch(error => {
          console.error('Error fetching data:', error);
        });
    };

    // Fetch data immediately and then set interval
    fetchData();
    const intervalId = setInterval(fetchData, 1000);

    // Cleanup interval on component unmount
    return () => clearInterval(intervalId);
  }, []);
    return (
    <div className="container">
      <div style={{ flex: 7 }}>
        <PieChart 
          series={[
            {
              data: sentiment,
            },
          ]}
          width={600}
          height={400}
        />
      </div>
      <div style={{ flex: 3 }}>
      <List>
          {message.map(item => (
          <ListItem key={item.id}>
              {item.latest_post_time}
              <br />
              {item.message_content} 
              <br />
              @{item.thread_url}
          </ListItem>
        ))}
      </List>
      </div>
    </div>
  )
}

export default App
