/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.koloboke.wordcount;

import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.ObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;
import gnu.trove.map.hash.TObjectIntHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Ordering.natural;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.openjdk.jol.info.GraphLayout.parseInstance;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 20, time = 1)
@Measurement(iterations = 20, time = 1)
@Threads(1)
@Fork(3)
@State(Scope.Benchmark)
public class WordCount {

    static String[] words;
    static int expectedSize;
    static long wordsRetentionSize;
    static {
        try {
            URL url = WordCount.class.getClassLoader().getResource("war_and_peace.txt");
            InputStreamReader reader = new InputStreamReader(url.openStream(), UTF_8);
            List<String> wordsList = new ArrayList<>();
            try (Scanner scanner = new Scanner(reader)) {
                scanner.useDelimiter("\\s+");
                for (; scanner.hasNext(); ) {
                    wordsList.add(scanner.next());
                }
            }
            words = wordsList.toArray(new String[0]);
            if (words.length == 0)
                throw new AssertionError();
            Object[] distinctWords = Arrays.stream(words).distinct().toArray();
            expectedSize = distinctWords.length;
            wordsRetentionSize = parseInstance(distinctWords).totalSize();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static HashConfig config(int loadLevel) {
        switch (loadLevel) {
            case 1: return config(0.066, 0.1, 0.134, 2.0);
            case 2: return config(0.133, 0.2, 0.267, 2.0);
            case 3: return config(0.2, 0.3, 0.4, 2.0);
            case 4: return config(0.266, 0.4, 0.534, 2.0);
            case 5: return config(0.33, 0.5, 0.67, 2.0);
            case 6: return config(0.4, 0.6, 0.8, 2.0);
            case 7: return config(0.466, 0.7, 0.934, 2.0);
            case 8: return config(0.64, 0.8, 0.96, 1.5);
            case 9: return config(0.79, 0.9, 0.99, 1.25);
            default: throw new AssertionError();
        }
    }

    private static HashConfig config(double minLoad, double targetLoad, double maxLoad,
            double growthFactor) {
        HashConfig config = HashConfig.getDefault().withGrowthFactor(growthFactor);
        if (minLoad < config.getMinLoad()) {
            return config.withMinLoad(minLoad).withTargetLoad(targetLoad).withMaxLoad(maxLoad);
        } else {
            return config.withMaxLoad(maxLoad).withTargetLoad(targetLoad).withMinLoad(minLoad);
        }
    }

    @Param({"1", "2", "3", "4", "5", "6", "7", "8", "9"})
    private int loadLevel;

    @State(Scope.Benchmark)
    public static class HashMapState {
        public Map<String, Integer> freq;

        @Setup
        public void createMap(WordCount wc) {
            double loadFactor = 0.1 * wc.loadLevel;
            int initialCapacity = (int) (expectedSize / loadFactor) + 1;
            freq = new HashMap<>(initialCapacity, (float) loadFactor);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int hashMap(HashMapState s) {
        Map<String, Integer> freq = s.freq;
        for (String word : words) {
            freq.merge(word, 1, (c, one) -> c + 1);
        }
        return freq.getOrDefault("map", 0);
    }

    @State(Scope.Benchmark)
    public static class AtomicIntegerState {
        public Map<String, AtomicInteger> freq;

        @Setup
        public void createMap(WordCount wc) {
            double loadFactor = 0.1 * wc.loadLevel;
            int initialCapacity = (int) (expectedSize / loadFactor) + 1;
            freq = new HashMap<>(initialCapacity, (float) loadFactor);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int atomicInteger(AtomicIntegerState s) {
        Map<String, AtomicInteger> freq = s.freq;
        for (String word : words) {
            freq.computeIfAbsent(word, w -> new AtomicInteger(0)).incrementAndGet();
        }
        return freq.getOrDefault("long", new AtomicInteger(0)).get();
    }

    static class MutableInt {
        int value = 0;
        public void increment () { ++value;      }
        public int  get ()       { return value; }
    }

    @State(Scope.Benchmark)
    public static class MutableIntState {
        public Map<String, MutableInt> freq;

        @Setup
        public void createMap(WordCount wc) {
            double loadFactor = 0.1 * wc.loadLevel;
            int initialCapacity = (int) (expectedSize / loadFactor) + 1;
            freq = new HashMap<>(initialCapacity, (float) loadFactor);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int mutableInt(MutableIntState s) {
        Map<String, MutableInt> freq = s.freq;
        for (String word : words) {
            freq.computeIfAbsent(word, w -> new MutableInt()).increment();
        }
        return freq.getOrDefault("mutable", new MutableInt()).get();
    }

    @State(Scope.Benchmark)
    public static class TroveState {
        public TObjectIntHashMap<String> freq;

        @Setup
        public void createMap(WordCount wc) {
            freq = new TObjectIntHashMap<>(expectedSize, 0.1f * wc.loadLevel);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int trove(TroveState s) {
        TObjectIntHashMap<String> freq = s.freq;
        for (String word : words) {
            freq.adjustOrPutValue(word, 1, 1);
        }
        return freq.get("trove");
    }

    @State(Scope.Benchmark)
    public static class KolobokeState {
        public ObjIntMap<String> freq;

        @Setup
        public void createMap(WordCount wc) {
            freq = HashObjIntMaps.getDefaultFactory()
                    .withHashConfig(config(wc.loadLevel))
                    .newUpdatableMap(expectedSize);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int koloboke(KolobokeState s) {
        ObjIntMap<String> freq = s.freq;
        for (String word : words) {
            freq.addValue(word, 1);
        }
        return freq.getInt("koloboke");
    }

    @State(Scope.Benchmark)
    public static class KolobokeCompileState {
        public WordCountMap freq;

        @Setup
        public void createMap(WordCount wc) {
            HashConfig config = config(wc.loadLevel);
            if (config.getGrowthFactor() == 2.0) {
                freq = SparseWordCountMap.withConfigAndExpectedSize(config, expectedSize);
            } else {
                freq = DenseWordCountMap.withConfigAndExpectedSize(config, expectedSize);
            }
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int kolobokeCompile(KolobokeCompileState s) {
        WordCountMap freq = s.freq;
        for (String word : words) {
            freq.addValue(word, 1);
        }
        return freq.getInt("koloboke");
    }

    @State(Scope.Benchmark)
    public static class EclipseState {
        public MutableObjectIntMap<String> freq;

        @Setup
        public void createMap(WordCount wc) {
            double loadFactor = 0.1 * (wc.loadLevel + 1);
            int initialCapacity = (int) (expectedSize / loadFactor);
            freq = new ObjectIntHashMap<>(initialCapacity);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int eclipse(EclipseState s) {
        MutableObjectIntMap<String> freq = s.freq;
        for (String word : words) {
            freq.addToValue(word, 1);
        }
        return freq.get("eclipse");
    }

    @State(Scope.Benchmark)
    public static class HppcState {
        public com.carrotsearch.hppc.ObjectIntHashMap<String> freq;

        @Setup
        public void createMap(WordCount wc) {
            double loadFactor = 0.1 * wc.loadLevel;
            int initialCapacity = (int) (expectedSize / loadFactor) + 1;
            freq = new com.carrotsearch.hppc.ObjectIntHashMap<>(
                    initialCapacity, (float) loadFactor);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int hppc(HppcState s) {
        com.carrotsearch.hppc.ObjectIntHashMap<String> freq = s.freq;
        for (String word : words) {
            freq.addTo(word, 1);
        }
        return freq.get("hppc");
    }

    @State(Scope.Benchmark)
    public static class HppcRtState {
        public com.carrotsearch.hppcrt.maps.ObjectIntHashMap<String> freq;

        @Setup
        public void createMap(WordCount wc) {
            double loadFactor = 0.1 * wc.loadLevel;
            int initialCapacity = (int) (expectedSize / loadFactor) + 1;
            freq = new com.carrotsearch.hppcrt.maps.ObjectIntHashMap<>(
                    initialCapacity, (float) loadFactor);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int hppcRt(HppcRtState s) {
        com.carrotsearch.hppcrt.maps.ObjectIntHashMap<String> freq = s.freq;
        for (String word : words) {
            freq.addTo(word, 1);
        }
        return freq.get("hppc");
    }

    @State(Scope.Benchmark)
    public static class FastutilState {
        public Object2IntOpenHashMap<String> freq;

        @Setup
        public void createMap(WordCount wc) {
            freq = new Object2IntOpenHashMap<>(expectedSize, 0.1f * wc.loadLevel);
        }

        @Setup(Level.Invocation)
        public void clearMap() {
            freq.clear();
        }
    }

    @Benchmark
    public int fastutil(FastutilState s) {
        Object2IntOpenHashMap<String> freq = s.freq;
        for (String word : words) {
            freq.addTo(word, 1);
        }
        return freq.getInt("fastutil");
    }

    public static void main(String[] args) {
        Table<String, Integer, Long> results = TreeBasedTable.create(natural(), natural());
        WordCount wc = new WordCount();
        for (int loadLevel = 1; loadLevel <= 9; loadLevel++) {
            wc.loadLevel = loadLevel;
            {
                HashMapState hashMapState = new HashMapState();
                hashMapState.createMap(wc);
                wc.hashMap(hashMapState);
                long retentionSize =
                        parseInstance(hashMapState.freq).totalSize() - wordsRetentionSize;
                results.put("hashMap", loadLevel, retentionSize);
            }
            {
                AtomicIntegerState atomicIntegerState = new AtomicIntegerState();
                atomicIntegerState.createMap(wc);
                wc.atomicInteger(atomicIntegerState);
                long retentionSize =
                        parseInstance(atomicIntegerState.freq).totalSize() - wordsRetentionSize;
                results.put("atomicInteger", loadLevel, retentionSize);
            }
            {
                MutableIntState mutableIntState = new MutableIntState();
                mutableIntState.createMap(wc);
                wc.mutableInt(mutableIntState);
                long retentionSize =
                        parseInstance(mutableIntState.freq).totalSize() - wordsRetentionSize;
                results.put("mutableInt", loadLevel, retentionSize);
            }
            {
                TroveState troveState = new TroveState();
                troveState.createMap(wc);
                wc.trove(troveState);
                long retentionSize =
                        parseInstance(troveState.freq).totalSize() - wordsRetentionSize;
                results.put("trove", loadLevel, retentionSize);
            }
            {
                KolobokeState kolobokeState = new KolobokeState();
                kolobokeState.createMap(wc);
                wc.koloboke(kolobokeState);
                long retentionSize =
                        parseInstance(kolobokeState.freq).totalSize() - wordsRetentionSize;
                results.put("koloboke", loadLevel, retentionSize);
            }
            {
                KolobokeCompileState kolobokeCompileState = new KolobokeCompileState();
                kolobokeCompileState.createMap(wc);
                wc.kolobokeCompile(kolobokeCompileState);
                long retentionSize =
                        parseInstance(kolobokeCompileState.freq).totalSize() - wordsRetentionSize;
                results.put("kolobokeCompile", loadLevel, retentionSize);
            }
            {
                EclipseState eclipseState = new EclipseState();
                eclipseState.createMap(wc);
                wc.eclipse(eclipseState);
                long retentionSize =
                        parseInstance(eclipseState.freq).totalSize() - wordsRetentionSize;
                results.put("eclipse", loadLevel, retentionSize);
            }
            {
                HppcState hppcState = new HppcState();
                hppcState.createMap(wc);
                wc.hppc(hppcState);
                long retentionSize =
                        parseInstance(hppcState.freq).totalSize() - wordsRetentionSize;
                results.put("hppc", loadLevel, retentionSize);
            }
            {
                HppcRtState hppcRtState = new HppcRtState();
                hppcRtState.createMap(wc);
                wc.hppcRt(hppcRtState);
                long retentionSize =
                        parseInstance(hppcRtState.freq).totalSize() - wordsRetentionSize;
                results.put("hppcRt", loadLevel, retentionSize);
            }
            {
                FastutilState fastutilState = new FastutilState();
                fastutilState.createMap(wc);
                wc.fastutil(fastutilState);
                long retentionSize =
                        parseInstance(fastutilState.freq).totalSize() - wordsRetentionSize;
                results.put("fastutil", loadLevel, retentionSize);
            }
        }
        results.rowMap().forEach((algo, levelResults) -> {
            levelResults.forEach((loadLevel, retentionSize) -> {
                System.out.printf("%s\t%d\t%d\n", algo, loadLevel, retentionSize);
            });
        });
    }
}
