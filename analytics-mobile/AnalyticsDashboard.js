// Reactive REST Controller with WebFlux
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {
    
    private final AnalyticsService analyticsService;
    private final WebSocketService webSocketService;
    
    public AnalyticsController(AnalyticsService analyticsService, WebSocketService webSocketService) {
        this.analyticsService = analyticsService;
        this.webSocketService = webSocketService;
    }
    
    @GetMapping(value = "/metrics/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Metric>> streamMetrics() {
        return analyticsService.getRealTimeMetrics()
                .map(metric -> ServerSentEvent.builder(metric).build())
                .delayElements(Duration.ofSeconds(1));
    }
    
    @GetMapping("/search")
    public Mono<SearchResults> searchMetrics(@RequestParam String query,
                                             @RequestParam(defaultValue = "0") int page) {
        return analyticsService.searchMetrics(query, page);
    }
    
    @PostMapping("/predict")
    public Mono<PredictionResult> predictTrend(@RequestBody PredictionRequest request) {
        return analyticsService.predictTrend(request);
    }
}

// Reactive service with multiple data sources
@Service
public class AnalyticsService {
    
    private final ReactiveMongoTemplate mongoTemplate;
    private final WebClient webClient;
    private final ElasticsearchRestTemplate elasticsearchTemplate;
    
    public Flux<Metric> getRealTimeMetrics() {
        // Combine multiple data sources reactively
        return Flux.merge(
            getDatabaseMetrics(),
            getApiMetrics(),
            getStreamMetrics()
        ).buffer(Duration.ofSeconds(5))
         .flatMap(this::aggregateMetrics);
    }
    
    private Flux<Metric> getDatabaseMetrics() {
        return mongoTemplate.find(Query.query(new Criteria()), Metric.class)
                .sample(Duration.ofSeconds(2));
    }
    
    private Flux<Metric> getApiMetrics() {
        return webClient.get()
                .uri("/external/metrics")
                .retrieve()
                .bodyToFlux(Metric.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)));
    }
    
    public Mono<SearchResults> searchMetrics(String query, int page) {
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.multiMatchQuery(query, "name", "description", "tags"))
                .withPageable(PageRequest.of(page, 20))
                .build();
        
        return Mono.fromCallable(() -> elasticsearchTemplate.search(searchQuery, Metric.class))
                .subscribeOn(Schedulers.boundedElastic())
                .map(searchHits -> new SearchResults(
                    searchHits.getSearchHits().stream()
                            .map(SearchHit::getContent)
                            .collect(Collectors.toList()),
                    searchHits.getTotalHits()
                ));
    }
    
    public Mono<PredictionResult> predictTrend(PredictionRequest request) {
        // Call serverless ML prediction function
        return webClient.post()
                .uri("https://us-central1-your-project.cloudfunctions.net/predict-trend")
                .bodyValue(request)
                .retrieve()
                .bodyToMono(PredictionResult.class);
    }
}

// React Native mobile app for analytics
import React, { useState, useEffect } from 'react';
import { View, Text, StyleSheet, ScrollView, RefreshControl } from 'react-native';
import { LineChart, BarChart } from 'react-native-chart-kit';
import { WebSocket } from 'react-native-websocket';

const AnalyticsDashboard = () => {
  const [metrics, setMetrics] = useState([]);
  const [refreshing, setRefreshing] = useState(false);
  
  useEffect(() => {
    // Fetch initial data
    fetchAnalyticsData();
    
    // Set up WebSocket for real-time updates
    const ws = new WebSocket('ws://your-server.com/ws/analytics');
    
    ws.onopen = () => {
      console.log('WebSocket connected');
    };
    
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setMetrics(prev => [data, ...prev.slice(0, 49)]); // Keep last 50 metrics
    };
    
    return () => ws.close();
  }, []);
  
  const fetchAnalyticsData = async () => {
    try {
      const response = await fetch('https://your-server.com/api/analytics/metrics');
      const data = await response.json();
      setMetrics(data);
    } catch (error) {
      console.error('Error fetching analytics:', error);
    }
  };
  
  const onRefresh = async () => {
    setRefreshing(true);
    await fetchAnalyticsData();
    setRefreshing(false);
  };
  
  return (
    <ScrollView 
      refreshControl={
        <RefreshControl refreshing={refreshing} onRefresh={onRefresh} />
      }
    >
      <View style={styles.container}>
        <Text style={styles.title}>Real-time Analytics Dashboard</Text>
        
        {metrics.length > 0 && (
          <>
            <LineChart
              data={{
                labels: metrics.map((_, i) => i.toString()),
                datasets: [{
                  data: metrics.map(m => m.value)
                }]
              }}
              width={350}
              height={200}
              chartConfig={chartConfig}
            />
            
            <View style={styles.statsContainer}>
              <Text style={styles.stat}>Current: {metrics[0]?.value || 0}</Text>
              <Text style={styles.stat}>Average: {
                (metrics.reduce((sum, m) => sum + m.value, 0) / metrics.length).toFixed(2)
              }</Text>
              <Text style={styles.stat}>Max: {
                Math.max(...metrics.map(m => m.value))
              }</Text>
            </View>
          </>
        )}
      </View>
    </ScrollView>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 20,
    backgroundColor: '#f5f5f5',
  },
  title: {
    fontSize: 24,
    fontWeight: 'bold',
    marginBottom: 20,
  },
  statsContainer: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginTop: 20,
  },
  stat: {
    fontSize: 16,
    fontWeight: '600',
  },
});

const chartConfig = {
  backgroundColor: '#ffffff',
  backgroundGradientFrom: '#ffffff',
  backgroundGradientTo: '#ffffff',
  decimalPlaces: 2,
  color: (opacity = 1) => `rgba(0, 123, 255, ${opacity})`,
};

export default AnalyticsDashboard;
