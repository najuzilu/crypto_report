class SqlQueries:
    time_table_insert = """
        INSERT into public.{} (
            close_time, hour,
            day, week, month,
            year, weekday
        )
        SELECT distinct(close_time),
            extract(hour from close_time),
            extract(day from close_time),
            extract(week from close_time),
            extract(month from close_time),
            extract(year from close_time),
            extract(dayofweek from close_time)
        FROM public.staging_crypto;
    """

    articles_table_insert = """
        INSERT INTO public.{} (
            author, content, description,
            title, url, url_to_image, published_at,
            published_date
        )
        SELECT distinct author, content, description,
            title, url, urlToImage, publishedAt,
            published_date
        FROM
            public.staging_news;
    """

    sources_table_insert = """
        INSERT INTO public.{} (
            id, name
        )
        SELECT DISTINCT(source_id), source_name
        FROM public.staging_news;
    """

    asset_base_table_insert = """
        INSERT INTO public.{}(
            id, symbol, name,
            fiat, route
        )
        SELECT
            distinct base_id, base_symbol, base_name,
            base_fiat, base_route
        FROM public.staging_crypto;
    """

    asset_quote_table_insert = """
        INSERT INTO public.{}(
            id, symbol, name,
            fiat, route
        )
        SELECT
            distinct quote_id, quote_symbol, quote_name,
            quote_fiat, quote_route
        FROM public.staging_crypto;
    """

    asset_markets_table_insert = """
        INSERT INTO public.{} (
            id, exchange, pair, active,
            route
        )
        SELECT
            distinct markets_id, markets_exchange,
            markets_pair, markets_active, markets_route
        FROM public.staging_crypto;
    """

    candlestick_table_insert = """
        INSERT INTO public.candlestick (
            ohlc_id,
            symbol,
            base_id,
            quote_id,
            route,
            markets_id,
            close_date,
            close_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            quote_volume,
            article_id,
            source_id,
            sentiment,
            positive_score,
            negative_score,
            mixed_score,
            neutral_score
        )
        SELECT
            id,
            symbol,
            base_id,
            quote_id,
            route,
            markets_id,
            close_date,
            close_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            quote_volume,
            article_id,
            source_id,
            sentiment,
            positive_score,
            negative_score,
            mixed_score,
            neutral_score
        FROM
            (
                SELECT
                    articles.id AS article_id,
                    news.source_id,
                    news.published_date,
                    news.sentiment,
                    news.positive_score,
                    news.negative_score,
                    news.mixed_score,
                    news.neutral_score
            FROM
                public.articles articles
            INNER JOIN
                public.staging_news news
            ON  articles.author=news.author AND
                articles.title=news.title and
                articles.published_date=news.published_date) test
        FULL JOIN public.staging_crypto crypto
        ON crypto.close_date=test.published_date;
        """
