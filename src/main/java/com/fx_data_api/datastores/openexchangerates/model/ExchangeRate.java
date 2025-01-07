package com.fx_data_api.datastores.openexchangerates.model;

import java.math.BigDecimal;

public record ExchangeRate(String baseCurreny, String quoteCurrency, BigDecimal rate, ExchangeRateType rateType) {

	public String currencyPair() {
		return baseCurreny + "/" + quoteCurrency;
	}
	
}
