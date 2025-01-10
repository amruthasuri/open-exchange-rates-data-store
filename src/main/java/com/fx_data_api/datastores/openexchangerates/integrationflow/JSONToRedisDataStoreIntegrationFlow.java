package com.fx_data_api.datastores.openexchangerates.integrationflow;

import java.io.File;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.json.JsonToObjectTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fx_data_api.datastores.openexchangerates.model.OpenExchangeRatesData;

@Configuration
public class JSONToRedisDataStoreIntegrationFlow {
    
	private static final String EXCHANGE_RATES_REDIS_STREAM_NAME = "exchange-rates-stream";

	private static final Logger logger = LoggerFactory.getLogger(JSONToRedisDataStoreIntegrationFlow.class);
	
	@Value("${OPEN_EXCHANGE_RATES_JSON_DIR}")
	private String exchangeRatesJsonDir;
	
    private static final String CURRENCY_PAIR_COMBINER = "/";

    private FileReadingMessageSource jsonFilesReader() {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(new File(exchangeRatesJsonDir));
        logger.debug( "message: {}" , source.receive() );
        return source;
    }

    @Bean
    public IntegrationFlow exchageRatesJSONDataToRedisFlow(StringRedisTemplate redisTemplate) {
        return IntegrationFlow.from(jsonFilesReader(), c -> c.poller(Pollers.fixedDelay(1000)))
                .transform(new JsonToObjectTransformer(OpenExchangeRatesData.class))  // Transform JSON to ExchangeRateRecord
                .handle(pushToRedisStream(redisTemplate))  // Push to Redis Stream
                .get();
    }

    private GenericHandler<Object> pushToRedisStream(StringRedisTemplate redisTemplate) {
        return (payload, headers) -> {
            OpenExchangeRatesData exchangeRateData = (OpenExchangeRatesData) payload;
            String baseCurrency = exchangeRateData.base();
            Map<String, BigDecimal> rates = exchangeRateData.rates();
            long timestamp = System.currentTimeMillis();  // Get current timestamp in milliseconds
            String streamName = EXCHANGE_RATES_REDIS_STREAM_NAME;  // Single Redis stream for all rates
            logger.debug("Push the exchange rates to Redis Stream");

            rates.forEach((quoteCurrency, exchangeRate) -> {
                String currencyPair = createCurrencyPair(baseCurrency, quoteCurrency);
                String exchangeRateString = exchangeRate.toPlainString();
                
                // Create a message map including rate, timestamp, and currency pair
                Map<String, String> message = new HashMap<>();
                message.put("currencyPair", currencyPair);
                message.put("rate", exchangeRateString);
                message.put("timestamp", String.valueOf(timestamp));

                // Push the message to the Redis Stream
                logger.debug("Push the exchange rate to Redis Stream for currency pair {}: {}", currencyPair, exchangeRateString);
                redisTemplate.opsForStream().add(streamName, message);  // Add to Redis Stream
            });

            return null;
        };
    }

	private String createCurrencyPair(String baseCurrecy, String quoteCurrency) {
		return baseCurrecy + CURRENCY_PAIR_COMBINER + quoteCurrency;
	}
}
