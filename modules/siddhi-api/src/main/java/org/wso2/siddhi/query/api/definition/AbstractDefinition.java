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

import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.exception.AttributeAlreadyExistException;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDefinition implements ExecutionPlan {
/*实现执行计划接口，执行计划接口本身没有定义什么接口方法*/
    protected String id;
    protected List<Attribute> attributeList = new ArrayList<Attribute>();
    /*一个字段的名字和类型，使用Attribute来封装*/

/*检查属性的目的就是怕有重复的字段*/
    protected void checkAttribute(String attributeName) {
        for (Attribute attribute : attributeList) {
            if (attribute.getName().equals(attributeName)) {
                throw new AttributeAlreadyExistException(attributeName + " is already defined for with type " + attribute.getType() + " for " + id);
            }
        }
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }

    public Attribute.Type getAttributeType(String attributeName) {
        for (Attribute attribute : attributeList) {
            if (attribute.getName().equals(attributeName)) {
                return attribute.getType();
            }
        }
        throw new AttributeNotExistException("Cannot find attribute type as " + attributeName + " does not exist in "+id);
    }
    /*流定义的字段是有先后顺序的，使用list来装这些字段，这样做其实有好处的，后面会提到
    * 提供获取字段位置的方法*/
    public int getAttributePosition(String attributeName) {
        for (int i = 0, attributeListSize = attributeList.size(); i < attributeListSize; i++) {
            Attribute attribute = attributeList.get(i);
            if (attribute.getName().equals(attributeName)) {
                return i;
            }
        }
        throw new AttributeNotExistException("Cannot get attribute position as " + attributeName + " does not exist in "+id);
    }


    public String[] getAttributeNameArray() {
        int attributeListSize = attributeList.size();
        String[] attributeNameArray = new String[attributeListSize];
        for (int i = 0; i < attributeListSize; i++) {
            attributeNameArray[i] = attributeList.get(i).getName();
        }
        return attributeNameArray;
    }/*转换成字符串数组，拿到字段的名称列表*/

    public String getId() {
        return id;
    }
}
