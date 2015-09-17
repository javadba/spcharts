/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blazedb.spark.reports;

import javafx.application.Platform;
import javafx.beans.property.StringProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.scene.Parent;
import javafx.scene.Scene;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.nio.charset.StandardCharsets;


/**
 * Class that handles the update of the CSS on the scene or any parent.
 *
 * Since in JavaFX, stylesheets can only be loaded from files or URLs, it implements a handler to create a magic "internal:stylesheet.css" url for our css string
 * see : https://github.com/fxexperience/code/blob/master/FXExperienceTools/src/com/fxexperience/tools/caspianstyler/CaspianStylerMainFrame.java
 * and : http://stackoverflow.com/questions/24704515/in-javafx-8-can-i-provide-a-stylesheet-from-a-string
 */
public class FXCSSUpdater {

    // URL Handler to create magic "internal:stylesheet.css" url for our css string
    {
        URL.setURLStreamHandlerFactory(new StringURLStreamHandlerFactory());
    }

    private static class StringHolder {
        public String str;
    }

    private final StringHolder cssHolder = new StringHolder();

    private Scene scene;

    public FXCSSUpdater(Scene scene) {
        this.scene = scene;
    }

    public void bindCss(final StringProperty cssProperty){
        cssProperty.addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
                cssHolder.str = cssProperty.get();
                Platform.runLater(new Runnable() {
                    public void run() {
                        scene.getStylesheets().clear();
                        scene.getStylesheets().add("internal:stylesheet.css");
                    }
                });
            }
        });
    }

    public void applyCssToParent(Parent parent){
//        parent.getStylesheets().clear();
        scene.getStylesheets().add("internal:stylesheet.css");
    }

    /**
     * URLConnection implementation that returns the css string property, as a stream, in the getInputStream method.
     */
    private class StringURLConnection extends URLConnection {
        public StringURLConnection(URL url){
            super(url);
        }

        @Override
        public void connect() throws IOException {}

        @Override public InputStream getInputStream() throws IOException {
            return new ByteArrayInputStream(cssHolder.str.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * URL Handler to create magic "internal:stylesheet.css" url for our css string
     */
    private class StringURLStreamHandlerFactory implements URLStreamHandlerFactory {

        URLStreamHandler streamHandler = new URLStreamHandler(){
            @Override
            protected URLConnection openConnection(URL url) throws IOException {
                if (url.toString().toLowerCase().endsWith(".css")) {
                    return new StringURLConnection(url);
                }
                throw new FileNotFoundException();
            }
        };

        @Override
        public URLStreamHandler createURLStreamHandler(String protocol) {
            if ("internal".equals(protocol)) {
                return streamHandler;
            }
            return null;
        }
    }
}