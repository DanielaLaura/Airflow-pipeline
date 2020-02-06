class SqlQueries:
    bq_check_githubarchive_day = ("""
    SELECT
          table_id
        FROM
          `githubarchive.day.__TABLES_SUMMARY__`
        WHERE
          table_id = "{{ yesterday_ds_nodash }}"
 
    """)
    bq_check_hackernews_full = ("""
    SELECT
          FORMAT_TIMESTAMP("%Y%m%d", timestamp ) AS date
        FROM
          `bigquery-public-data.hacker_news.full`
        WHERE
          type = 'story'
          AND FORMAT_TIMESTAMP("%Y%m%d", timestamp ) = "{{ yesterday_ds_nodash }}"
        LIMIT
          1
    """)
    bq_write_to_github_daily_metrics = ("""
    SELECT
          date,
          repo,
          SUM(IF(type='WatchEvent', 1, NULL)) AS stars,
          SUM(IF(type='ForkEvent',  1, NULL)) AS forks
        FROM (
          SELECT
            FORMAT_TIMESTAMP("%Y%m%d", created_at) AS date,
            actor.id as actor_id,
            repo.name as repo,
            type
          FROM
            `githubarchive.day.{{ yesterday_ds_nodash }}`
          WHERE type IN ('WatchEvent','ForkEvent')
        )
        GROUP BY
          date,
          repo
    """)
    bq_write_to_github_agg = (""" 
    SELECT
          "{2}" as date,
          repo,
          SUM(stars) as stars_last_28_days,
          SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{4}") 
            AND TIMESTAMP("{3}") , 
            stars, null)) as stars_last_7_days,
          SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{3}") 
            AND TIMESTAMP("{3}") , 
            stars, null)) as stars_last_1_day,
          SUM(forks) as forks_last_28_days,
          SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{4}") 
            AND TIMESTAMP("{3}") , 
            forks, null)) as forks_last_7_days,
          SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{3}") 
            AND TIMESTAMP("{3}") , 
            forks, null)) as forks_last_1_day
        FROM
          `{0}.{1}.github_daily_metrics`
        WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{5}") 
        AND TIMESTAMP("{3}") 
        GROUP BY
          date,
          repo
    """)
    bq_write_to_hackernews_agg = ("""
     SELECT
      FORMAT_TIMESTAMP("%Y%m%d", timestamp) AS date,
      `by` AS submitter,
      id as story_id,
      REGEXP_EXTRACT(url, "(https?://github.com/[^/]*/[^/#?]*)") as url,
      SUM(score) as score
    FROM
      `bigquery-public-data.hacker_news.full`
    WHERE
      type = 'story'
      AND timestamp>'{{ yesterday_ds }}'
      AND timestamp<'{{ ds }}'
      AND url LIKE '%https://github.com%'
      AND url NOT LIKE '%github.com/blog/%'
    GROUP BY
      date,
      submitter,
      story_id,
      url
    """)

