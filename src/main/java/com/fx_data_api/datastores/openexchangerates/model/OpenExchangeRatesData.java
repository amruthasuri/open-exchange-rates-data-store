package com.fx_data_api.datastores.openexchangerates.model;

import java.math.BigDecimal;
import java.util.Map;

public record OpenExchangeRatesData(String disclaimer, String license, Long timestamp, String base, Map<String, BigDecimal> rates) {
	
}
