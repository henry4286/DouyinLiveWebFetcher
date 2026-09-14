"""Verify profile observations across real protobuf routing and NDJSON boundaries."""
import gzip
import io
import json
import unittest
from types import SimpleNamespace

from collector_events import actor_fields, NdjsonSink, MAX_SAFE_COUNT
from liveMan import DouyinLiveWebFetcher
from protobuf.douyin import (
    User, Image, FollowInfo, PayGrade, FansClub, FansClubData, Common,
    ChatMessage, MemberMessage, GiftMessage, GiftStruct, LikeMessage,
    SocialMessage, FansclubMessage, Message, Response, PushFrame,
)


class ViewerProfileTests(unittest.TestCase):
    def test_profile_survives_six_event_types_and_ndjson(self):
        user = User(
            id=18446744073709551615, sec_uid='MS4wLjABsynthetic',
            nick_name='测试🙂', display_id='test_123',
            avatar_thumb=Image(url_list_list=['https://example.invalid/a.webp?x=1&y=2']),
            follow_info=FollowInfo(follower_count=123, following_count=45, follow_status=1),
            pay_grade=PayGrade(level=12),
            fans_club=FansClub(data=FansClubData(level=6, user_fans_club_status=1, anchor_id=987654321)),
        )
        common = Common(room_id=123, create_time=1789263000000)
        cases = [
            ('WebcastChatMessage', ChatMessage(common=common,user=user,content='测试评论🙂'), 'comment'),
            ('WebcastMemberMessage', MemberMessage(common=common,user=user), 'enter_room'),
            ('WebcastGiftMessage', GiftMessage(common=common,user=user,gift_id=7,
                gift=GiftStruct(name='测试礼物'),repeat_count=3,combo_count=2,repeat_end=1,group_id=9), 'gift'),
            ('WebcastLikeMessage', LikeMessage(common=common,user=user,count=2), 'like'),
            ('WebcastSocialMessage', SocialMessage(common=common,user=user,action=1), 'social'),
            ('WebcastFansclubMessage', FansclubMessage(common_info=common,user=user,content='测试粉丝团'), 'fansclub'),
        ]
        messages = [Message(method=method,payload=msg.SerializeToString(),msg_id=i+100)
                    for i,(method,msg,_) in enumerate(cases)]
        frame = PushFrame(payload=gzip.compress(Response(messages_list=messages).SerializeToString()))
        output=io.StringIO()
        fetcher=DouyinLiveWebFetcher('456',event_sink=NdjsonSink(output))
        fetcher._wsOnMessage(object(),frame.SerializeToString())
        events=[json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([e['type'] for e in events],[c[2] for c in cases])
        expected = {'displayId':'test_123','avatarUrl':'https://example.invalid/a.webp?x=1&y=2',
                    'followerCount':123,'followingCount':45,'followStatusRaw':1,'payGradeLevel':12,
                    'fansClubLevel':6,'fansClubStatusRaw':1,'fansClubAnchorId':'987654321'}
        for i,e in enumerate(events):
            with self.subTest(event=e['type']):
                self.assertEqual(e['actor']['observedProfile'],expected)
                self.assertEqual(e['actor']['observedSourceUserId'],'18446744073709551615')
                self.assertEqual(e['actor']['observedSecUid'],'MS4wLjABsynthetic')
                self.assertIsNone(e['actor']['sourceUserId'])
                self.assertEqual(e['quality']['actorId'],'unavailable')
                self.assertEqual(e['eventId'],str(i+100))
                self.assertEqual(e['source']['method'],cases[i][0])
        self.assertEqual(events[0]['payload']['content'],'测试评论🙂')
        self.assertEqual({k:events[2]['payload'][k] for k in ('repeatCount','comboCount','repeatEnd','groupId','countSemantics','giftValue')},
                         {'repeatCount':3,'comboCount':2,'repeatEnd':1,'groupId':'9','countSemantics':'unverified','giftValue':None})

    def test_missing_fields_and_proto_zero_remain_unknown(self):
        for user in (None,User(),User(follow_info=FollowInfo(follower_count=0,following_count=0))):
            with self.subTest(user=type(user).__name__):
                actor,quality=actor_fields(user)
                self.assertNotIn('observedProfile',actor)
                self.assertEqual(quality,'unavailable')

    def test_profile_does_not_turn_placeholder_identity_into_verified_identity(self):
        actor,quality=actor_fields(User(id=111111,display_id='observed-handle',
                                      avatar_thumb=Image(url_list_list=['https://example.invalid/a'])) )
        self.assertNotIn('observedSourceUserId',actor)
        self.assertIsNone(actor['sourceUserId'])
        self.assertEqual(quality,'unavailable')
        self.assertEqual(actor['observedProfile']['displayId'],'observed-handle')

    def test_bad_avatar_urls_are_skipped_and_medium_can_supply_avatar(self):
        urls=['javascript:alert(1)','file:///private/a','https://user:pass@example.invalid/a',
              'https://example.invalid/a\nforged','https://[broken','https://example.invalid/'+'a'*4096]
        user=User(avatar_thumb=Image(url_list_list=urls),
                  avatar_medium=Image(url_list_list=['https://example.invalid/medium.webp']))
        self.assertEqual(actor_fields(user)[0]['observedProfile'],{'avatarUrl':'https://example.invalid/medium.webp'})

    def test_display_id_is_preserved_as_string_or_omitted_when_invalid(self):
        for value in ('001234567890123456789','normal_01'):
            self.assertEqual(actor_fields(User(display_id=value))[0]['observedProfile']['displayId'],value)
        for value in ('','a\nb','a b','x'*257):
            with self.subTest(valueLength=len(value)):
                self.assertNotIn('observedProfile',actor_fields(User(display_id=value))[0])

    def test_counts_do_not_lose_integer_precision_or_accept_boolean_values(self):
        for value in (-1,0,True,1.5,'123',MAX_SAFE_COUNT+1):
            user=SimpleNamespace(follow_info=SimpleNamespace(follower_count=value,following_count=value))
            with self.subTest(value=value):
                self.assertNotIn('observedProfile',actor_fields(user)[0])
        actor,_=actor_fields(User(follow_info=FollowInfo(follower_count=MAX_SAFE_COUNT)))
        self.assertEqual(actor['observedProfile']['followerCount'],MAX_SAFE_COUNT)


if __name__=='__main__':
    unittest.main()
