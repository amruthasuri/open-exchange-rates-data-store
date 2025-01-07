package com.fx_data_api.datastores.openexchangerates.integrationflow;

import java.io.File;
import java.math.BigDecimal;
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
                .handle(pushToRedisDataStore(redisTemplate))
                .get();
    }

	private GenericHandler<Object> pushToRedisDataStore(StringRedisTemplate redisTemplate) {
		return (payload, headers) -> {
			OpenExchangeRatesData exchangeRateData = (OpenExchangeRatesData) payload;
			String baseCurrecy = exchangeRateData.base();
		    Map<String, BigDecimal> rates = exchangeRateData.rates();
		    logger.debug("push the exchange rates to Redis");
		    rates.forEach((quoteCurrency,exchangeRate) -> {
		        String currencyPair = createCurrencyPair(baseCurrecy, quoteCurrency);
				String exchangeRateString = exchangeRate.toPlainString();
			    logger.debug("push the exchange rate to Redis {} {} ", currencyPair,exchangeRateString);
				redisTemplate.opsForValue().set(currencyPair,exchangeRateString);
		    });
		    return null;
		};
	}

	private String createCurrencyPair(String baseCurrecy, String quoteCurrency) {
		return baseCurrecy + CURRENCY_PAIR_COMBINER + quoteCurrency;
	}

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
