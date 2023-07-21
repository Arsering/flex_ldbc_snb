package org.ldbcoucil.snb.impls.workloads.graphscope;

import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.workloads.interactive.*;
import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcoucil.snb.impls.workloads.graphscope.operationhandlers.GraphScopeListOperationHandler;
import org.ldbcoucil.snb.impls.workloads.graphscope.operationhandlers.GraphScopeSingletonOperationHandler;
import org.ldbcoucil.snb.impls.workloads.graphscope.operationhandlers.GraphScopeUpdateOperationHandler;
import org.ldbcoucil.snb.impls.workloads.graphscope.util.HttpConfig;

import java.io.IOException;
import java.util.*;

final class Decoder {
    public Decoder(byte[] bs) {
        this.bs = bs;
        this.loc = 0;
        this.len = this.bs.length;
    }

    public static int get_int(byte[] bs, int loc) {
        int ret = (bs[loc + 3] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 2] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 1] & 0xff);
        ret <<= 8;
        ret |= (bs[loc] & 0xff);
        return ret;
    }

    public static long get_long(byte[] bs, int loc) {
        long ret = (bs[loc + 7] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 6] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 5] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 4] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 3] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 2] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 1] & 0xff);
        ret <<= 8;
        ret |= (bs[loc] & 0xff);
        return ret;
    }

    public long get_long() {
        long ret = get_long(this.bs, this.loc);
        this.loc += 8;
        return ret;
    }

    public int get_int() {
        int ret = get_int(this.bs, this.loc);
        this.loc += 4;
        return ret;
    }

    public byte get_byte() {
        return (byte) (bs[loc++] & 0xFF);
    }

    public String get_string() {
        int strlen = this.get_int();
        String ret = new String(this.bs, this.loc, strlen);
        this.loc += strlen;
        return ret;
    }

    public boolean empty() {
        return loc == len;
    }

    byte[] bs;
    int loc;
    int len;
}

final class Encoder {
    public Encoder(byte[] bs) {
        this.bs = bs;
        this.loc = 0;
    }

    public static int serialize_long(byte[] bytes, int offset, long value) {
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        return offset;
    }

    public static int serialize_int(byte[] bytes, int offset, int value) {
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        return offset;
    }

    public static int serialize_byte(byte[] bytes, int offset, byte value) {
        bytes[offset++] = value;
        return offset;
    }

    public static int serialize_bytes(byte[] bytes, int offset, byte[] value) {
        offset = serialize_int(bytes, offset, value.length);
        System.arraycopy(value, 0, bytes, offset, value.length);
        return offset + value.length;
    }

    public static int serialize_raw_bytes(byte[] bytes, int offset, byte[] value) {
        System.arraycopy(value, 0, bytes, offset, value.length);
        return offset + value.length;
    }

    public static byte[] serialize_long_byte(long value, byte type) {
        byte[] bytes = new byte[9];
        serialize_long(bytes, 0, value);
        serialize_byte(bytes, 8, type);
        return bytes;
    }

    public static byte[] serialize_long_long_byte(long a, long b, byte type) {
        byte[] bytes = new byte[17];
        serialize_long(bytes, 0, a);
        serialize_long(bytes, 8, b);
        serialize_byte(bytes, 16, type);
        return bytes;
    }

    public static byte[] serialize_long_int_byte(long a, int b, byte type) {
        byte[] bytes = new byte[13];
        serialize_long(bytes, 0, a);
        serialize_int(bytes, 8, b);
        serialize_byte(bytes, 12, type);
        return bytes;
    }

    public static byte[] serialize_long_long_long_byte(long a, long b, long c, byte type) {
        byte[] bytes = new byte[25];
        serialize_long(bytes, 0, a);
        serialize_long(bytes, 8, b);
        serialize_long(bytes, 16, c);
        serialize_byte(bytes, 24, type);
        return bytes;
    }

    public static byte[] serialize_long_long_int_byte(long a, long b, int c, byte type) {
        byte[] bytes = new byte[21];
        serialize_long(bytes, 0, a);
        serialize_long(bytes, 8, b);
        serialize_int(bytes, 16, c);
        serialize_byte(bytes, 20, type);
        return bytes;
    }

    public static byte[] serialize_long_string_byte(long value, String b, byte type) {
        byte[] b_ba = b.getBytes();
        byte[] bytes = new byte[13 + b_ba.length];
        serialize_long(bytes, 0, value);
        int offset = serialize_bytes(bytes, 8, b_ba);
        serialize_byte(bytes, offset, type);
        return bytes;
    }

    public void put_int(int value) {
        this.loc = serialize_int(this.bs, this.loc, value);
    }

    public void put_byte(byte value) {
        this.loc = serialize_byte(this.bs, this.loc, value);
    }

    public void put_long(long value) {
        this.loc = serialize_long(this.bs, this.loc, value);
    }

    public void put_bytes(byte[] bytes) {
        this.loc = serialize_bytes(this.bs, this.loc, bytes);
    }

    byte[] bs;
    int loc;
}

public class GraphScopeDb extends Db{
    HttpConfig config;

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {
        config = new HttpConfig();
        if(null == map.get("url")){
            throw new DbException("No URL provided");
        }
        config.setServerAddr(map.get("url"));
        if(null != map.get("readTimeout")){
            config.setReadTimeout(Integer.valueOf(map.get("readTimeout")));
        }
        if(null != map.get("connectTimeout")){
            config.setConnectTimeout(Integer.valueOf(map.get("connectTimeout")));
        }
        if(null != map.get("connectPoolMaxIdle")){
            config.setConnectPoolMaxIdle(Integer.valueOf(map.get("connectPoolMaxIdle")));
        }
        if(null != map.get("keepAliveDuration")){
            config.setKeepAliveDuration(Integer.valueOf(map.get("keepAliveDuration")));
        }
        if(null != map.get("maxRequestsPerHost")){
            config.setMaxRequestsPerHost(Integer.valueOf(map.get("maxRequestsPerHost")));
        }
        if(null != map.get("maxRequests")){
            config.setMaxRequests(Integer.valueOf(map.get("maxRequests")));
        }
    }

    @Override
    protected void onClose() throws IOException {

    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return new GraphScopeDbConnectionState(config);
    }


    public static class InteractiveQuery1 extends GraphScopeListOperationHandler<LdbcQuery1,LdbcQuery1Result>
    {
        @Override
        public List<LdbcQuery1Result> toResult(byte [] bs) {
            Decoder decoder = new Decoder(bs);
            List<LdbcQuery1Result> results = new ArrayList<>();
            while (!decoder.empty()) {
                long friendId = decoder.get_long();
                int distanceFromPerson = decoder.get_int();
                String friendLastName = decoder.get_string();
                long friendBirthday = decoder.get_long();
                long friendCreationDate = decoder.get_long();
                String friendGender = decoder.get_string();
                String friendBrowserUsed = decoder.get_string();
                String friendLocationIp = decoder.get_string();
                String friendCityName = decoder.get_string();

                String emails = decoder.get_string();
                String languages = decoder.get_string();

                int universityNum = decoder.get_int();

                List<LdbcQuery1Result.Organization> universities = new ArrayList<>(universityNum);
                for (int i = 0; i < universityNum; ++i) {
                    universities.add(new LdbcQuery1Result.Organization(decoder.get_string(), decoder.get_int(), decoder.get_string()));
                }

                int companyNum = decoder.get_int();

                List<LdbcQuery1Result.Organization> companies = new ArrayList<>(companyNum);
                for (int i = 0; i < companyNum; ++i) {
                    companies.add(new LdbcQuery1Result.Organization(decoder.get_string(), decoder.get_int(), decoder.get_string()));
                }
                results.add(new LdbcQuery1Result(friendId,
                    friendLastName,
                    distanceFromPerson,
                    friendBirthday,
                    friendCreationDate,
                    friendGender,
                    friendBrowserUsed,
                    friendLocationIp,
                    Arrays.asList(emails.split(";")),
                    Arrays.asList(languages.split(";")),
                    friendCityName,
                    universities,
                    companies));
            }

            return results;
        }


        @Override
        protected byte[] serialization(LdbcQuery1 o) {
            return Encoder.serialize_long_string_byte(o.getPersonIdQ1(), o.getFirstName(), (byte) 1);
        }
    }
    public static class InteractiveQuery2 extends GraphScopeListOperationHandler<LdbcQuery2, LdbcQuery2Result>
    {
        @Override
        public List<LdbcQuery2Result> toResult(byte [] bs) {
            Decoder decoder = new Decoder(bs);
            List<LdbcQuery2Result> results = new ArrayList<>();
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                String personFirstName = decoder.get_string();
                String personLastName = decoder.get_string();
                long messageId = decoder.get_long();
                String messageContent = decoder.get_string();
                long messageCreationDate = decoder.get_long();
                results.add(new LdbcQuery2Result(personId,
                    personFirstName,
                    personLastName,
                    messageId,
                    messageContent,
                    messageCreationDate));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery2 o) {
            return Encoder.serialize_long_long_byte(o.getPersonIdQ2(), o.getMaxDate().getTime(), (byte) 2);
        }
    }

    public static class InteractiveQuery3 extends GraphScopeListOperationHandler<LdbcQuery3, LdbcQuery3Result>
    {
        @Override
        public List<LdbcQuery3Result> toResult(byte [] bs) {
            Decoder decoder = new Decoder(bs);
            List<LdbcQuery3Result> results = new ArrayList<>();
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                String personFirstName = decoder.get_string();
                String personLastName = decoder.get_string();
                long xCount = decoder.get_long();
                long yCount = decoder.get_long();
                long count = decoder.get_long();
                results.add(new LdbcQuery3Result(personId,
                    personFirstName,
                    personLastName,
                    xCount,
                    yCount,
                    count ));
            }
            return results;
        }


        @Override
        protected byte[] serialization(LdbcQuery3 o) {
            byte[] countryXName = o.getCountryXName().getBytes();
            byte[] countryYName = o.getCountryYName().getBytes();
            byte[] result = new byte[29 + countryXName.length + countryYName.length];
            Encoder encoder = new Encoder(result);
            encoder.put_long(o.getPersonIdQ3());
            encoder.put_bytes(countryXName);
            encoder.put_bytes(countryYName);
            encoder.put_long(o.getStartDate().getTime());
            encoder.put_int(o.getDurationDays());
            encoder.put_byte((byte) 3);
            return result;
        }
    }


    public static class InteractiveQuery4 extends GraphScopeListOperationHandler<LdbcQuery4, LdbcQuery4Result>
    {
        @Override
        public List<LdbcQuery4Result> toResult(byte [] bs) {
            Decoder decoder = new Decoder(bs);
            List<LdbcQuery4Result> results = new ArrayList<>();
            while (!decoder.empty()) {
                String tagName = decoder.get_string();
                int postCount = decoder.get_int();
                results.add(new LdbcQuery4Result(tagName, postCount));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery4 o) {
            return Encoder.serialize_long_long_int_byte(o.getPersonIdQ4(), o.getStartDate().getTime(), o.getDurationDays(), (byte) 4);
        }
    }

    public static class InteractiveQuery5 extends GraphScopeListOperationHandler<LdbcQuery5, LdbcQuery5Result>
    {
        @Override
        public List<LdbcQuery5Result> toResult(byte [] bs) {
            Decoder decoder = new Decoder(bs);
            List<LdbcQuery5Result> results = new ArrayList<>();
            while (!decoder.empty()) {
                String forumTitle = decoder.get_string();
                int postCount = decoder.get_int();
                results.add(new LdbcQuery5Result(forumTitle, postCount));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery5 o) {
            return Encoder.serialize_long_long_byte(o.getPersonIdQ5(), o.getMinDate().getTime(), (byte) 5);
        }
    }

    public static class InteractiveQuery6 extends GraphScopeListOperationHandler<LdbcQuery6, LdbcQuery6Result>
    {
        @Override
        public List<LdbcQuery6Result> toResult(byte [] bs) {
            List<LdbcQuery6Result> results = new ArrayList<>();
            Decoder decoder = new Decoder(bs);
            while (!decoder.empty()) {
                String tagName = decoder.get_string();
                int postCount = decoder.get_int();
                results.add(new LdbcQuery6Result(tagName, postCount));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery6 o) {
            return Encoder.serialize_long_string_byte(o.getPersonIdQ6(), o.getTagName(), (byte) 6);
        }
    }

    public static class InteractiveQuery7 extends GraphScopeListOperationHandler<LdbcQuery7, LdbcQuery7Result>
    {
        @Override
        public List<LdbcQuery7Result> toResult(byte [] bs) {
            List<LdbcQuery7Result> results = new ArrayList<>();
            Decoder decoder = new Decoder(bs);
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                String personFirstName = decoder.get_string();
                String personLastName = decoder.get_string();
                long likeCreationDate = decoder.get_long();
                long messageId = decoder.get_long();
                String messageContent = decoder.get_string();
                int minuteLatency = decoder.get_int();
                boolean isNew = decoder.get_byte() != (byte) 0;
                results.add(new LdbcQuery7Result(personId, personFirstName, personLastName, likeCreationDate, messageId, messageContent, minuteLatency, isNew));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery7 o) {
            return Encoder.serialize_long_byte(o.getPersonIdQ7(), (byte) 7);
        }
    }

    public static class InteractiveQuery8 extends GraphScopeListOperationHandler<LdbcQuery8, LdbcQuery8Result>
    {
        @Override
        public List<LdbcQuery8Result> toResult(byte [] bs) {
            List<LdbcQuery8Result> results = new ArrayList<>();
            Decoder decoder = new Decoder(bs);
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                String personFirstName = decoder.get_string();
                String personLastName = decoder.get_string();
                long commentCreationDate = decoder.get_long();
                long commentId = decoder.get_long();
                String commentContent = decoder.get_string();
                results.add(new LdbcQuery8Result(personId,
                    personFirstName,
                    personLastName,
                    commentCreationDate,
                    commentId,
                    commentContent));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery8 o) {
            return Encoder.serialize_long_byte(o.getPersonIdQ8(), (byte) 8);
        }
    }

    public static class InteractiveQuery9 extends GraphScopeListOperationHandler<LdbcQuery9, LdbcQuery9Result>
    {
        @Override
        public List<LdbcQuery9Result> toResult(byte [] bs) {
            List<LdbcQuery9Result> results = new ArrayList<>();
            Decoder decoder = new Decoder(bs);
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                String personFirstName = decoder.get_string();
                String personLastName = decoder.get_string();
                long messageId = decoder.get_long();
                String messageContent = decoder.get_string();
                long messageCreationDate = decoder.get_long();
                results.add(new LdbcQuery9Result(
                    personId,
                    personFirstName,
                    personLastName,
                    messageId,
                    messageContent,
                    messageCreationDate ));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery9 o) {
            return Encoder.serialize_long_long_byte(o.getPersonIdQ9(), o.getMaxDate().getTime(), (byte) 9);
        }
    }

    public static class InteractiveQuery10 extends GraphScopeListOperationHandler<LdbcQuery10, LdbcQuery10Result>
    {
        @Override
        public List<LdbcQuery10Result> toResult(byte [] bytes) {
            List<LdbcQuery10Result> results = new ArrayList<>();
            Decoder decoder = new Decoder(bytes);
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                String personFirstName = decoder.get_string();
                String personLastName = decoder.get_string();
                int commonInterestScore = decoder.get_int();
                String personGender = decoder.get_string();
                String personCityName = decoder.get_string();
                results.add(new LdbcQuery10Result(
                        personId,
                        personFirstName,
                        personLastName,
                        commonInterestScore,
                        personGender,
                        personCityName));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery10 o) {
            return Encoder.serialize_long_int_byte(o.getPersonIdQ10(), o.getMonth(), (byte) 10);
        }
    }

    public static class InteractiveQuery11 extends GraphScopeListOperationHandler<LdbcQuery11, LdbcQuery11Result>
    {
        @Override
        public List<LdbcQuery11Result> toResult(byte [] bytes) {
            List<LdbcQuery11Result> results = new ArrayList<>();
            Decoder decoder = new Decoder(bytes);
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                String personFirstName = decoder.get_string();
                String personLastName = decoder.get_string();
                String organizationName = decoder.get_string();
                int organizationWorkFromYear = decoder.get_int();
                results.add(new LdbcQuery11Result(
                        personId,
                        personFirstName,
                        personLastName,
                        organizationName,
                        organizationWorkFromYear ));

            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery11 o) {
            byte[] countryName = o.getCountryName().getBytes();
            byte[] result = new byte[17 + countryName.length];
            Encoder encoder = new Encoder(result);
            encoder.put_long(o.getPersonIdQ11());
            encoder.put_bytes(countryName);
            encoder.put_int(o.getWorkFromYear());
            encoder.put_byte((byte) 11);
            return result;
        }
    }

    public static class InteractiveQuery12 extends GraphScopeListOperationHandler<LdbcQuery12, LdbcQuery12Result>
    {
        @Override
        public List<LdbcQuery12Result> toResult(byte [] bytes) {
            List<LdbcQuery12Result> results = new ArrayList<>();
            Decoder decoder = new Decoder(bytes);
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                String personFirstName = decoder.get_string();
                String personLastName = decoder.get_string();
                int tagNum = decoder.get_int();
                List<String> tagNames = new ArrayList<>(tagNum);
                for (int i = 0; i < tagNum; ++i) {
                    tagNames.add(decoder.get_string());
                }
                int replyCount = decoder.get_int();
                results.add(new LdbcQuery12Result(
                        personId,
                        personFirstName,
                        personLastName,
                        tagNames,
                        replyCount));

            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery12 o) {
            return Encoder.serialize_long_string_byte(o.getPersonIdQ12(), o.getTagClassName(), (byte) 12);
        }
    }

    public static class InteractiveQuery13 extends GraphScopeSingletonOperationHandler<LdbcQuery13, LdbcQuery13Result>
    {
        @Override
        public LdbcQuery13Result toResult(byte [] bytes) {
            return new LdbcQuery13Result(Decoder.get_int(bytes, 0));
        }

        @Override
        protected byte[] serialization(LdbcQuery13 o) {
            return Encoder.serialize_long_long_byte(o.getPerson1IdQ13StartNode(), o.getPerson2IdQ13EndNode(), (byte) 13);
        }
    }


    public static class InteractiveQuery14 extends GraphScopeListOperationHandler<LdbcQuery14, LdbcQuery14Result>
    {
        @Override
        public List<LdbcQuery14Result> toResult(byte [] bytes) {
            List<LdbcQuery14Result> results = new ArrayList<>();
            Decoder decoder = new Decoder(bytes);
            while (!decoder.empty()) {
                int pathLen = decoder.get_int();
                List<Long> path = new ArrayList<>(pathLen);
                for (int i = 0; i < pathLen; ++i) {
                    path.add(decoder.get_long());
                }
                double pathWeight = Double.longBitsToDouble(decoder.get_long());
                results.add(new LdbcQuery14Result(path, pathWeight));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcQuery14 o) {
            return Encoder.serialize_long_long_byte(o.getPerson1IdQ14StartNode(), o.getPerson2IdQ14EndNode(), (byte) 14);
        }
    }

    public static class ShortQuery1PersonProfile extends GraphScopeSingletonOperationHandler<LdbcShortQuery1PersonProfile,LdbcShortQuery1PersonProfileResult> {
        @Override
        public LdbcShortQuery1PersonProfileResult toResult(byte [] bs) {
            Decoder decoder = new Decoder(bs);
            String firstName = decoder.get_string();
            String lastName = decoder.get_string();
            String locationIP  = decoder.get_string();
            String gender = decoder.get_string();
            long creationDate = decoder.get_long();
            long birthday = decoder.get_long();
            String browserUsed = decoder.get_string();

            long cityId = decoder.get_long();

            return new LdbcShortQuery1PersonProfileResult(
                    firstName,
                    lastName,
                    birthday,
                    locationIP,
                    browserUsed,
                    cityId,
                    gender,
                    creationDate);
        }

        @Override
        protected byte[] serialization(LdbcShortQuery1PersonProfile o) {
            return Encoder.serialize_long_byte(o.getPersonIdSQ1(), (byte) 15);
        }
    }

    public static class ShortQuery2PersonPosts extends GraphScopeListOperationHandler<LdbcShortQuery2PersonPosts,LdbcShortQuery2PersonPostsResult>
    {
        @Override
        public List<LdbcShortQuery2PersonPostsResult> toResult(byte[] bs) {
            Decoder decoder = new Decoder(bs);
            List<LdbcShortQuery2PersonPostsResult> results = new ArrayList<>();
            while (!decoder.empty()) {
                long messageId = decoder.get_long();
                long messageCreationDate = decoder.get_long();
                String messageContent = decoder.get_string();
                long originalPostId = decoder.get_long();
                long originalPostAuthorId = decoder.get_long();
                String originalPostAuthorFirstName = decoder.get_string();
                String originalPostAuthorLastName = decoder.get_string();
                results.add(
                    new LdbcShortQuery2PersonPostsResult(
                    messageId,
                    messageContent,
                    messageCreationDate,
                    originalPostId,
                    originalPostAuthorId,
                    originalPostAuthorFirstName,
                    originalPostAuthorLastName)
                );
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcShortQuery2PersonPosts o) {
            return Encoder.serialize_long_byte(o.getPersonIdSQ2(), (byte) 16);
        }
    }

    public static class ShortQuery3PersonFriends extends GraphScopeListOperationHandler<LdbcShortQuery3PersonFriends,LdbcShortQuery3PersonFriendsResult>
    {

        @Override
        public List<LdbcShortQuery3PersonFriendsResult> toResult(byte[] bs) {
            List<LdbcShortQuery3PersonFriendsResult> results = new ArrayList<>();
            Decoder decoder = new Decoder(bs);
            while (!decoder.empty()) {
                long personId = decoder.get_long();
                long creationDate = decoder.get_long();
                String firstName = decoder.get_string();
                String lastName = decoder.get_string();
                results.add(new LdbcShortQuery3PersonFriendsResult(
                        personId,
                        firstName,
                        lastName,
                        creationDate));
            }
            return results;
        }

        @Override
        protected byte[] serialization(LdbcShortQuery3PersonFriends o) {
            return Encoder.serialize_long_byte(o.getPersonIdSQ3(), (byte) 17);
        }
    }

    public static class ShortQuery4MessageContent extends GraphScopeSingletonOperationHandler<LdbcShortQuery4MessageContent,LdbcShortQuery4MessageContentResult>
    {
        @Override
        public LdbcShortQuery4MessageContentResult toResult(byte[] bs) {
            long messageCreationDate = Decoder.get_long(bs, 0);
            int strlen = Decoder.get_int(bs, 8);
            String messageContent = new String(bs, 12, strlen);
            return new LdbcShortQuery4MessageContentResult(
                    messageContent,
                    messageCreationDate );
        }

        @Override
        protected byte[] serialization(LdbcShortQuery4MessageContent o) {
            return Encoder.serialize_long_byte(o.getMessageIdContent(), (byte) 18);
        }
    }

    public static class ShortQuery5MessageCreator extends GraphScopeSingletonOperationHandler<LdbcShortQuery5MessageCreator,LdbcShortQuery5MessageCreatorResult>
    {
        @Override
        public LdbcShortQuery5MessageCreatorResult toResult(byte[] bs) {
            Decoder decoder = new Decoder(bs);
            long personId = decoder.get_long();
            String firstName = decoder.get_string();
            String lastName = decoder.get_string();
            return new LdbcShortQuery5MessageCreatorResult(
                    personId,
                    firstName,
                    lastName );
        }

        @Override
        protected byte[] serialization(LdbcShortQuery5MessageCreator o) {
            return Encoder.serialize_long_byte(o.getMessageIdCreator(), (byte) 19);
        }
    }

    public static class ShortQuery6MessageForum extends GraphScopeSingletonOperationHandler<LdbcShortQuery6MessageForum,LdbcShortQuery6MessageForumResult>
    {
        @Override
        public LdbcShortQuery6MessageForumResult toResult( byte [] bs )
        {
            Decoder decoder = new Decoder(bs);
            long forumId = decoder.get_long();
            String forumTitle = decoder.get_string();
            long moderatorId = decoder.get_long();
            String moderatorFirstName = decoder.get_string();
            String moderatorLastName = decoder.get_string();
            return new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName );
        }

        @Override
        protected byte[] serialization(LdbcShortQuery6MessageForum o) {
            return Encoder.serialize_long_byte(o.getMessageForumId(), (byte) 20);
        }
    }

    public static class ShortQuery7MessageReplies extends GraphScopeListOperationHandler<LdbcShortQuery7MessageReplies,LdbcShortQuery7MessageRepliesResult>
    {
        @Override
        public List<LdbcShortQuery7MessageRepliesResult> toResult(byte[] bs) {
            List<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();
            Decoder decoder = new Decoder(bs);
            while (!decoder.empty()) {
                long commentId = decoder.get_long();
                long commentCreationDate = decoder.get_long();
                String commentContent = decoder.get_string();
                long replyAuthorId = decoder.get_long();
                String replyAuthorFirstName = decoder.get_string();
                String replyAuthorLastName = decoder.get_string();
                boolean replyAuthorKnowsOriginalMessageAuthor = (decoder.get_byte() == (byte) 1);
                results.add(
                    new LdbcShortQuery7MessageRepliesResult(
                    commentId,
                    commentContent,
                    commentCreationDate,
                    replyAuthorId,
                    replyAuthorFirstName,
                    replyAuthorLastName,
                    replyAuthorKnowsOriginalMessageAuthor )
                );
            }
            return results;
        }


        @Override
        protected byte[] serialization(LdbcShortQuery7MessageReplies o) {
            return Encoder.serialize_long_byte(o.getMessageRepliesId(), (byte) 21);
        }
    }

    public static class Update1AddPerson extends GraphScopeUpdateOperationHandler<LdbcUpdate1AddPerson>
    {

        @Override
        protected byte[] serialization(LdbcUpdate1AddPerson o) {
            int length = 0;
            length += 8; // person id
            byte[] firstName = o.getPersonFirstName().getBytes();
            length += (4 + firstName.length); // firstname
            byte[] lastName = o.getPersonLastName().getBytes();
            length += (4 + lastName.length); // lastname
            byte[] gender = o.getGender().getBytes();
            byte[] locationIp = o.getLocationIp().getBytes();
            byte[] browserUsed = o.getBrowserUsed().getBytes();
            length += (4 + gender.length); // gender
            length += 16; // birthday, creationDate
            length += (4 + locationIp.length); // locationIp
            length += (4 + browserUsed.length); // browserUsed
            length += 8; // cityId
            byte[] languages = String.join(";", o.getLanguages()).getBytes();
            length += (4 + languages.length); // languages
            byte[] emails = String.join(";", o.getEmails()).getBytes();
            length += (4 + emails.length); // emails
            length += (4 + o.getTagIds().size() * 8); // tagids
            length += (4 + o.getStudyAt().size() * 12); // studyAt
            length += (4 + o.getWorkAt().size() * 12); // workAt
            length += 1; // type

            byte[] result = new byte[length];
            Encoder encoder = new Encoder(result);
            encoder.put_long(o.getPersonId());
            encoder.put_bytes(firstName);
            encoder.put_bytes(lastName);
            encoder.put_bytes(gender);
            encoder.put_long(o.getBirthday().getTime());
            encoder.put_long(o.getCreationDate().getTime());
            encoder.put_bytes(locationIp);
            encoder.put_bytes(browserUsed);
            encoder.put_long(o.getCityId());
            encoder.put_bytes(languages);
            encoder.put_bytes(emails);
            encoder.put_int(o.getTagIds().size());
            for (Long tagId : o.getTagIds()) {
                encoder.put_long(tagId);
            }
            encoder.put_int(o.getStudyAt().size());
            for (LdbcUpdate1AddPerson.Organization v : o.getStudyAt()) {
                encoder.put_long(v.getOrganizationId());
                encoder.put_int(v.getYear());
            }
            encoder.put_int(o.getWorkAt().size());
            for (LdbcUpdate1AddPerson.Organization v : o.getWorkAt()) {
                encoder.put_long(v.getOrganizationId());
                encoder.put_int(v.getYear());
            }
            encoder.put_byte((byte) 22);
            return result;
        }
    }
    public static class Update2AddPostLike extends GraphScopeUpdateOperationHandler<LdbcUpdate2AddPostLike>
    {
        @Override
        protected byte[] serialization(LdbcUpdate2AddPostLike o) {
            return Encoder.serialize_long_long_long_byte(o.getPersonId(), o.getPostId(), o.getCreationDate().getTime(), (byte) 23);
        }
    }

    public static class Update3AddCommentLike extends GraphScopeUpdateOperationHandler<LdbcUpdate3AddCommentLike>
    {
        @Override
        protected byte[] serialization(LdbcUpdate3AddCommentLike o) {
            return Encoder.serialize_long_long_long_byte(o.getPersonId(), o.getCommentId(), o.getCreationDate().getTime(), (byte) 24);
        }
    }

    public static class Update4AddForum extends GraphScopeUpdateOperationHandler<LdbcUpdate4AddForum>
    {
        @Override
        protected byte[] serialization(LdbcUpdate4AddForum o) {
            byte[] forumTitle = o.getForumTitle().getBytes();
            // byte[] result = new byte[forumTitle.length + 33 + o.getTagIds().size() * 8];
            byte[] result = new byte[forumTitle.length + 29];
            Encoder encoder = new Encoder(result);
            encoder.put_long(o.getForumId());
            encoder.put_bytes(forumTitle);
            encoder.put_long(o.getCreationDate().getTime());
            encoder.put_long(o.getModeratorPersonId());
            /*
            encoder.put_int(o.getTagIds().size());
            for (Long tagId : o.getTagIds()) {
                encoder.put_long(tagId);
            }
             */
            encoder.put_byte((byte) 25);
            return result;
        }
    }
    public static class Update5AddForumMembership extends GraphScopeUpdateOperationHandler<LdbcUpdate5AddForumMembership>
    {
        @Override
        protected byte[] serialization(LdbcUpdate5AddForumMembership o) {
            return Encoder.serialize_long_long_long_byte(o.getForumId(), o.getPersonId(), o.getJoinDate().getTime(), (byte) 26);
        }
    }

    public static class Update6AddPost extends GraphScopeUpdateOperationHandler<LdbcUpdate6AddPost>
    {
        @Override
        protected byte[] serialization(LdbcUpdate6AddPost o) {
            int length = 0;
            length += 8; // author person id
            byte[] browserUsed = o.getBrowserUsed().getBytes();
            length += (4 + browserUsed.length); // browserUsed
            length += 8; // creationDate
            byte[] content = o.getContent().getBytes();
            length += (4 + content.length); // content
            length += 24; // country id, forum id, post id
            byte[] imageFile = o.getImageFile().getBytes();
            length += (4 + imageFile.length); // imageFile
            byte[] language = o.getLanguage().getBytes();
            length += (4 + language.length); // language;
            length += 4; // length
            byte[] locationIp = o.getLocationIp().getBytes();
            length += (4 + locationIp.length); // locationIp
            length += (4 + o.getTagIds().size() * 8); // tag ids
            length += 1; // type

            byte[] result = new byte[length];
            Encoder encoder = new Encoder(result);
            encoder.put_long(o.getAuthorPersonId());
            encoder.put_bytes(browserUsed);
            encoder.put_long(o.getCreationDate().getTime());
            encoder.put_bytes(content);
            encoder.put_long(o.getCountryId());
            encoder.put_long(o.getForumId());
            encoder.put_long(o.getPostId());
            encoder.put_bytes(imageFile);
            encoder.put_bytes(language);
            encoder.put_int(o.getLength());
            encoder.put_bytes(locationIp);
            encoder.put_int(o.getTagIds().size());
            for (Long tagId : o.getTagIds()) {
                encoder.put_long(tagId);
            }
            encoder.put_byte((byte) 27);
            return result;
        }
    }

    public static class Update7AddComment extends GraphScopeUpdateOperationHandler<LdbcUpdate7AddComment>
    {
        @Override
        protected byte[] serialization(LdbcUpdate7AddComment o) {
            int length = 0;
            length += 16; // author person id, comment id
            byte[] browserUsed = o.getBrowserUsed().getBytes();
            length += (4 + browserUsed.length); // browserUsed
            byte[] content = o.getContent().getBytes();
            length += (4 + content.length); // content
            length += 20; // country id, creation date, length
            byte[] locationIp = o.getLocationIp().getBytes();
            length += (4 + locationIp.length);
            length += 16; // reply to comment id, reply to post id
            // length += (4 + o.getTagIds().size() * 8); // tag ids
            length += 1; // type

            byte[] result = new byte[length];
            Encoder encoder = new Encoder(result);
            encoder.put_long(o.getAuthorPersonId());
            encoder.put_long(o.getCommentId());
            encoder.put_bytes(browserUsed);
            encoder.put_bytes(content);
            encoder.put_long(o.getCountryId());
            encoder.put_long(o.getCreationDate().getTime());
            encoder.put_int(o.getLength());
            encoder.put_bytes(locationIp);
            encoder.put_long(o.getReplyToCommentId());
            encoder.put_long(o.getReplyToPostId());
            /*
            encoder.put_int(o.getTagIds().size());
            for (Long tagId : o.getTagIds()) {
                encoder.put_long(tagId);
            }
             */
            encoder.put_byte((byte) 28);
            return result;
        }
    }

    public static class Update8AddFriendship extends GraphScopeUpdateOperationHandler<LdbcUpdate8AddFriendship>
    {
        @Override
        protected byte[] serialization(LdbcUpdate8AddFriendship o) {
            return Encoder.serialize_long_long_long_byte(o.getPerson1Id(), o.getPerson2Id(), o.getCreationDate().getTime(), (byte) 29);
        }
    }
}
