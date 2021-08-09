CREATE TABLE IF NOT EXISTS public.staging_crypto (
	base_fiat boolean,
	base_id int4 NOT NULL,
	base_name varchar(256),
	base_route varchar(256),
	base_symbol varchar(3),
	id int4 NOT NULL,
	markets_active boolean,
	markets_exchange varchar(256),
	markets_id int8 NOT NULL,
	markets_pair varchar(6),
	markets_route varchar(256),
	quote_fiat boolean,
	quote_id int4 NOT NULL,
	quote_name varchar(256),
	quote_route varchar(256),
	quote_symbol varchar(3),
	"route" varchar(256),
	symbol varchar(256),
	open_price numeric(18,0),
	high_price numeric(18,0),
	low_price numeric(18,0),
	close_price numeric(18,0),
	volume numeric(18,0),
	quote_volume numeric(18,0)
);

CREATE TABLE IF NOT EXISTS public.staging_news (
	author varchar(256),
	content varchar(256),
	"description" varchar(256),
	publishAt timestamp NOT NULL,
	source_id varchar(256) NOT NULL,
	source_name varchar(256),
	title varchar(256) NOT NULL,
	"url" varchar(256),
	urlToImage varchar(256),
	sentiment numeric(18,0),
	positive_score numeric(18,0),
	negative_score numeric(18,0),
	mixed_score numeric(18,0),
	neutral_score numeric(18,0)
);

CREATE TABLE IF NOT EXISTS public.asset_base (
	id int4 NOT NULL,
	symbol varchar(3),
	name varchar(256),
	fiat boolean,
	"route" varchar(256),
	CONSTRAINT asset_base_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.asset_quote (
	id  int4 NOT NULL,
	symbol varchar(3),
	name varchar(256),
	fiat boolean,
	"route" varchar(256),
	CONSTRAINT asset_quote_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.asset_markets (
	id int4,
	exchange varchar(256),
	pair varchar(6),
	active boolean,
	"route" varchar(256),
	CONSTRAINT asset_markets_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.candlestick (
	id bigint identity(0, 1),
	ohlc_id int4,
	symbol varchar(256),
	base_id  int4,
	quote_id int4,
	"route" varchar(256),
	markets_id int4,
	close_date date,
	close_time timestamp,
	open_price numeric(18,0),
	high_price numeric(18,0),
	low_price numeric(18,0),
	close_price numeric(18,0),
	volume numeric(18,0),
	quote_volume numeric(18,0),
	article_id int8,
	source_id int4,
	sentiment numeric(18,0),
	positive_score numeric(18,0),
	negative_score numeric(18,0),
	mixed_score numeric(18,0),
	neutral_score numeric(18,0),
	CONSTRAINT candlestick_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."time" (
	close_time timestamp NOT NULL,
	hour int4,
	day int4,
	week int4,
	month int4,
	year int4,
	weekday int4,
	CONSTRAINT close_time_pkey PRIMARY KEY (close_time)
);

CREATE TABLE IF NOT EXISTS public.articles (
	id bigint identity(0, 1),
	author varchar(256),
	content varchar(256),
	"description" varchar(256),
	title varchar(256),
	url varchar(256),
	urlToImage varchar(256),
	CONSTRAINT articles_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.sources (
	id varchar(256) NOT NULL,
	name varchar(256),
	CONSTRAINT sources_pkey PRIMARY KEY (id)
);
