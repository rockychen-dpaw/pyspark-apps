from collections import OrderedDict

from .. import database

userid_cache = {}
def adb2cUserid2email(userid,auth2_dburls=None,cache_size=3000):
    try:
        #userid is cached
        email = userid_cache[userid]
        userid_cache.move_to_end(userid,last=True)
        return email
    except:
        if not auth2_dburls:
            return userid

        email = None
        for dburl in auth2_dburls:
            with database.Database(dburl).get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("select a.email from auth_user a join social_auth_usersocialauth b on a.id =b.user_id where b.uid='{}'".format(userid))
                    data = cursor.fetchone()
                    if data:
                        email = data[0]
                        break
                    else:
                        continue

        if not email:
            email = userid

        userid_cache[userid] = email
        if cache_size > 0 and len(userid_cache) > cache_size:
            #cache is too large, remove one item
            userid_cache.popitem(last=False)

        return email

transformers = [adb2cUserid2email]
