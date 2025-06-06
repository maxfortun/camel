/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.language.groovy;

import java.io.LineNumberReader;
import java.io.StringReader;

import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.codehaus.groovy.control.messages.Message;
import org.codehaus.groovy.control.messages.SyntaxErrorMessage;

/**
 * Exception when validating groovy scripts.
 */
public class GroovyValidationException extends Exception {

    private final String script;

    public GroovyValidationException(String script, Throwable cause) {
        super(cause);
        this.script = script;
    }

    public String getScript() {
        return script;
    }

    public int getIndex() {
        if (getCause() instanceof MultipleCompilationErrorsException me) {
            Message gm = me.getErrorCollector().getLastError();
            if (gm instanceof SyntaxErrorMessage sem) {
                LineNumberReader lr = new LineNumberReader(new StringReader(script));
                int pos = -1;
                for (int i = 1; i < sem.getCause().getLine(); i++) {
                    try {
                        String line = lr.readLine();
                        pos += line.length() + System.lineSeparator().length();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                pos += sem.getCause().getStartColumn();
                return pos;
            }
        }
        return -1;
    }

}
