/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.util.snapshot;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.util.ExceptionUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Serializer used by {@link SnapshotService} to do Object to Byte[] conversion and vise-versa
 */
public class ByteSerializer {
    private static final Logger log = Logger.getLogger(ByteSerializer.class);

    private ByteSerializer() {
    }

    public static byte[] objectToByte(Object obj, SiddhiAppContext siddhiAppContext) {
        long start = System.currentTimeMillis();
        byte[] out = null;

        if (obj != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = null;
            try {
                oos = new ObjectOutputStream(baos);
                oos.writeObject(obj);
                oos.reset();
                oos.flush();
                out = baos.toByteArray();
            } catch (IOException e) {
                log.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                        " Error when writing byte array.", e);
                return null;
            } finally {
                try {
                    baos.close();
                } catch (IOException e) {
                    log.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                            " Error when closing ByteArrayOutputStream.", e);
                }
                if (oos != null) {
                    try {
                        oos.close();
                    } catch (IOException e) {
                        log.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                                " Error when closing ObjectOutputStream.", e);
                    }
                }
            }
        }
        long end = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            if (out != null) {
                log.debug("SiddhiApp '" + siddhiAppContext.getName() + "' encoded in :" + (end - start) +
                        " msec, with a size of: " + out.length + " bytes.");
            }
        }
        return out;
    }

    public static Object byteToObject(byte[] bytes, SiddhiAppContext siddhiAppContext) {
        if (log.isDebugEnabled()) {
            log.debug("SiddhiApp '" + siddhiAppContext.getName() + "'. is tobe decoded with a size of: " +
                    bytes.length + " bytes.");
        }
        long start = System.currentTimeMillis();
        Object out = null;
        if (bytes != null) {
            try {
                ByteArrayInputStream bios = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bios);
                out = ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                log.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                        " Error when writing to object.", e);
                return null;
            }
        }
        long end = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug("Decoded in :" + (end - start) + " msec");
        }
        return out;
    }
}
