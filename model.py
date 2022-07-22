from psycopg2 import connect
from datetime import datetime
from psycopg2.extras import RealDictCursor
import pandas as pd
from catboost import CatBoostClassifier, Pool
import numpy as np

start_time = datetime.now()
user_id = 202
'''
connection = connect(host="postgres.lab.karpov.courses",
                     port=6432, dbname='startml',
                     user="robot-startml-ro",
                     password="pheiph0hahj1Vaif")

with connection.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute(f"""SELECT user_id, * FROM public.feed_data WHERE user_id = %(user)s""", {'user': user_id})
    feed = cur.fetchall()
    cur.close()
connection.close()
'''
# Добыча данных
user_df = pd.read_sql(
    """SELECT * 
    FROM public.user_data
    """,
    con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
)

post_text_df = pd.read_sql(
    """SELECT * 
    FROM public.post_text_df
    """,
    con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
)
'''
feed_df = pd.read_sql(
    """SELECT * 
    FROM public.feed_data
    LIMIT 200000
    """,
    con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
)
# Фичаинжиниринг
# Преобразуем datetime в год, месяц и день для использования при рекомендации
feed_df['year'] = feed_df['timestamp'].dt.year
feed_df['month'] = feed_df['timestamp'].dt.month
feed_df['day'] = feed_df['timestamp'].dt.day
feed_df['weekday'] = feed_df['timestamp'].dt.weekday
user_activity = feed_df[feed_df['action'] == 'view'].groupby(['user_id'], as_index=False).agg({'action': 'count'})
user_like = feed_df[feed_df['action'] == 'like'].groupby(['user_id'], as_index=False).agg({'action': 'count'})
# Новая фича для юзеров, активность т.е. количество просмотренных постов
user_df1 = user_df.merge(user_activity, on='user_id').rename({'action': 'activity'}, axis=1)
user_df2 = user_df1.merge(user_like, on='user_id').rename({'action': 'activity_like'}, axis=1)
user_df2['like_rate'] = user_df2['activity_like'] / user_df2['activity']
user_df3 = user_df2.drop(['gender', 'country', 'city', 'os', 'source', 'activity', 'activity_like'], axis=1)
# Фича для постов, сколько лайков собрал пост
post_like = feed_df[feed_df['action'] == 'like'].merge(post_text_df.loc[:, ['post_id', 'topic']], on='post_id'). \
    groupby(['post_id'], as_index=False).agg({'action': 'count'})

post_text_df1 = post_text_df.merge(post_like, on='post_id').rename({'action': 'activity_post_like'}, axis=1)
# Фича для постов, сколько раз пост смотрели
post_like = feed_df[feed_df['action'] == 'view'].merge(post_text_df.loc[:, ['post_id', 'topic']], on='post_id').\
            groupby(['post_id'], as_index=False).agg({'action': 'count'})
# Длина статьи len()
post_text_df1['text_len'] = post_text_df1.text.str.len()

post_text_df1_1 = post_text_df1.merge(post_like, on='post_id').rename({'action': 'activity_post'}, axis=1)
post_text_df1_1['interest'] = post_text_df1_1['activity_post_like'] / post_text_df1_1['activity_post']
post_text_df2 = post_text_df1_1.drop(['text', 'topic', 'activity_post_like', 'activity_post'], axis=1)
# Мержим все фичи
df_all1 = feed_df.merge(post_text_df2, on='post_id')
df_all2 = df_all1.merge(user_df3, how='inner', on='user_id')
df_all2 = df_all2.sort_values('timestamp').reset_index(drop=True)
df_all2 = df_all2[df_all2['action'] == 'view']
df_all3 = df_all2.drop(['timestamp', 'user_id', 'post_id', 'action', 'year'], axis=1)
# делим на трейн и на тест
train = df_all3.iloc[:-41688].copy()
test = df_all3.iloc[-41688:].copy()
category_columns = ['weekday', 'month', 'day', 'exp_group', 'text_len']
X_train = train.drop('target', axis=1)
X_test = test.drop('target', axis=1)
y_train = train['target']
y_test = test['target']
# Создание и обучение модели
catboost = CatBoostClassifier()
catboost.fit(X_train,
             y_train,
             cat_features=category_columns,
             )
# Сохранение модели
catboost.save_model('catboost_model',
                           format="cbm")
'''
# Подготовка для рекомендации
user_df_one = pd.read_sql(
    f"""SELECT * 
    FROM public.user_data
    WHERE user_id = {user_id}
    """,
    con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml")

feed_df = pd.read_sql(
    f"""SELECT * 
    FROM public.feed_data
    WHERE user_id = {user_id}
    """,
    con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml")

end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))
