/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.siddhi.query.api.definition;

public class StreamDefinition extends AbstractDefinition {
    /*流定义的结构其实非常简单，无非就是：
    * 1、stream起个名字，叫做streamID,y用于区别不同的流
    * 2、每个流有哪些字段，每个字段哪些类型（7种）
    *
    * 7种类型分别是：
    * int，double,float,long,boolean,String,type
    * 其中type 有别的用处，相当于object类型*/
    public StreamDefinition name(String streamId) {
        id = streamId;
        return this;
    }

    public StreamDefinition attribute(String attributeName, Attribute.Type type) {
        checkAttribute(attributeName);
        this.attributeList.add(new Attribute(attributeName, type));
        return this;
    }

    public String getStreamId() {
        return id;
    }

    @Override
    public String toString() {
        return "StreamDefinition{" +
               "streamId='" + id + '\'' +
               ", attributeList=" + attributeList +
               '}';
    }

}
