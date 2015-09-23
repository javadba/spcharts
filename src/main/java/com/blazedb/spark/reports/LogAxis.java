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

import javafx.animation.KeyFrame;
import javafx.animation.KeyValue;
import javafx.animation.Timeline;
import javafx.beans.binding.DoubleBinding;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.scene.chart.ValueAxis;
import javafx.util.Duration;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * LogAxis
//http://blog.dooapp.com/logarithmic-scale-strikes-back-in-javafx-20
//Edited by Vadim Levit & Benny Lutati for usage in AgentZero ( https://code.google.com/p/azapi-test/ )
 */
public class LogAxis extends ValueAxis<Double> {

    /**
     * The time of animation in ms
     */
    private static final double ANIMATION_TIME = 2000;
    private final Timeline lowerRangeTimeline = new Timeline();
    private final Timeline upperRangeTimeline = new Timeline();

    private final DoubleProperty logUpperBound = new SimpleDoubleProperty();
    private final DoubleProperty logLowerBound = new SimpleDoubleProperty();

    public LogAxis() {
        super(1, 100);
        bindLogBoundsToDefaultBounds();
    }

    public LogAxis(double lowerBound, double upperBound) {
        super(lowerBound, upperBound);
        try {
            validateBounds(lowerBound, upperBound);
            bindLogBoundsToDefaultBounds();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    /**
     * Bind our logarithmic bounds with the super class bounds, consider the base 10 logarithmic scale.
     */
    private void bindLogBoundsToDefaultBounds() {
        logLowerBound.bind(new DoubleBinding() {

            {
                super.bind(lowerBoundProperty());
            }

            @Override
            protected double computeValue() {
                return Math.log10(lowerBoundProperty().get());
            }
        });
        logUpperBound.bind(new DoubleBinding() {

            {
                super.bind(upperBoundProperty());
            }

            @Override
            protected double computeValue() {
                return Math.log10(upperBoundProperty().get());
            }
        });
    }

    /**
     * Validate the bounds by throwing an exception if the values are not conform to the mathematics log interval:
     * ]0,Double.MAX_VALUE]
     *
     * @param lowerBound
     * @param upperBound
     * @throws IllegalArgumentException
     */
    private void validateBounds(double lowerBound, double upperBound) throws IllegalArgumentException {
        if (lowerBound < 0 || upperBound < 0 || lowerBound > upperBound) {
            throw new IllegalArgumentException(
                    "The logarithmic range should be in (0,Double.MAX_VALUE) and the lowerBound should be less than the upperBound");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Double> calculateMinorTickMarks() {
        Double[] range = getRange();
        List<Double> minorTickMarksPositions = new ArrayList<Double>();
        if (range != null) {

            Double upperBound = range[1];
            double logUpperBound = Math.log10(upperBound.doubleValue());
            int minorTickMarkCount = getMinorTickCount();

            for (double i = 0; i <= logUpperBound; i += 1) {
                for (double j = 0; j <= 9; j += (1. / minorTickMarkCount)) {
                    double value = j * Math.pow(10, i);
                    minorTickMarksPositions.add(value);
                }
            }
        }
        return minorTickMarksPositions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Double> calculateTickValues(double length, Object range) {
        List<Double> tickPositions = new ArrayList<Double>();
        if (range != null) {
            Double lowerBound = ((Double[]) range)[0];
            Double upperBound = ((Double[]) range)[1];
            double logLowerBound = Math.log10(lowerBound.doubleValue());
            double logUpperBound = Math.log10(upperBound.doubleValue());

            for (double i = 0; i <= logUpperBound; i += 1) {
                for (double j = 1; j <= 9; j++) {
                    double value = j * Math.pow(10, i);
                    tickPositions.add(value);
                }
            }
        }
        return tickPositions;
    }

    @Override
    protected Double[] getRange() {
        return new Double[]{lowerBoundProperty().get(), upperBoundProperty().get()};
    }

    @Override
    protected String getTickMarkLabel(Double value) {
        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setMaximumIntegerDigits(6);
        formatter.setMinimumIntegerDigits(1);
        return formatter.format(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setRange(Object range, boolean animate) {
        if (range != null) {
            Double lowerBound = ((Double[]) range)[0];
            Double upperBound = ((Double[]) range)[1];
            try {
                validateBounds(lowerBound.doubleValue(), upperBound.doubleValue());
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
            if (animate) {
                try {
                    lowerRangeTimeline.getKeyFrames().clear();
                    upperRangeTimeline.getKeyFrames().clear();

                    lowerRangeTimeline.getKeyFrames()
                            .addAll(new KeyFrame(Duration.ZERO, new KeyValue(lowerBoundProperty(), lowerBoundProperty()
                                            .get())),
                                    new KeyFrame(new Duration(ANIMATION_TIME), new KeyValue(lowerBoundProperty(),
                                            lowerBound.doubleValue())));

                    upperRangeTimeline.getKeyFrames()
                            .addAll(new KeyFrame(Duration.ZERO, new KeyValue(upperBoundProperty(), upperBoundProperty()
                                            .get())),
                                    new KeyFrame(new Duration(ANIMATION_TIME), new KeyValue(upperBoundProperty(),
                                            upperBound.doubleValue())));
                    lowerRangeTimeline.play();
                    upperRangeTimeline.play();
                } catch (Exception e) {
                    lowerBoundProperty().set(lowerBound.doubleValue());
                    upperBoundProperty().set(upperBound.doubleValue());
                }
            }
            lowerBoundProperty().set(lowerBound.doubleValue());
            upperBoundProperty().set(upperBound.doubleValue());
        }
    }

    @Override
    public Double getValueForDisplay(double displayPosition) {
        double delta = logUpperBound.get() - logLowerBound.get();
        if (getSide().isVertical()) {
            return Math.pow(10, (((displayPosition - getHeight()) / -getHeight()) * delta) + logLowerBound.get());
        } else {
            return Math.pow(10, (((displayPosition / getWidth()) * delta) + logLowerBound.get()));
        }
    }

    @Override
    public double getDisplayPosition(Double value) {
        double delta = logUpperBound.get() - logLowerBound.get();
        double deltaV = Math.log10(value.doubleValue()) - logLowerBound.get();
        if (getSide().isVertical()) {
            return (1. - ((deltaV) / delta)) * getHeight();
        } else {
            return ((deltaV) / delta) * getWidth();
        }
    }
}