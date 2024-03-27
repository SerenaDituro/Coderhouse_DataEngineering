CREATE TABLE IF NOT EXISTS serenadituro_coderhouse.finanzas(
	symbol VARCHAR(10) NOT NULL,
    currency VARCHAR(30) NOT NULL,
    exchange_timezone VARCHAR(50) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    mic_code VARCHAR(10) NOT NULL,
    type VARCHAR(30) NOT NULL,
    datetime DATE NOT NULL,
    open_value FLOAT NOT NULL,
    high_value FLOAT NOT NULL,
    close_value FLOAT NOT NULL,
    volume INT NOT NULL,
    PRIMARY KEY(symbol,datetime)
);