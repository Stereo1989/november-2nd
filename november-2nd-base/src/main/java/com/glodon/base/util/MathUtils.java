package com.glodon.base.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.Random;

public class MathUtils {

    static SecureRandom cachedSecureRandom;

    static volatile boolean seeded;

    private static final Random RANDOM = new Random();

    private MathUtils() {
    }

    public static int roundUpInt(int x, int blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    public static long roundUpLong(long x, long blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    private static synchronized SecureRandom getSecureRandom() {
        if (cachedSecureRandom != null) {
            return cachedSecureRandom;
        }
        try {
            cachedSecureRandom = SecureRandom.getInstance("SHA1PRNG");
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
                        byte[] seed = sr.generateSeed(20);
                        synchronized (cachedSecureRandom) {
                            cachedSecureRandom.setSeed(seed);
                            seeded = true;
                        }
                    } catch (Exception e) {
                        warn("SecureRandom", e);
                    }
                }
            };

            try {
                Thread t = new Thread(runnable, "Generate Seed");
                t.setDaemon(true);
                t.start();
                Thread.yield();
                try {
                    t.join(400);
                } catch (InterruptedException e) {
                    warn("InterruptedException", e);
                }
                if (!seeded) {
                    byte[] seed = generateAlternativeSeed();
                    synchronized (cachedSecureRandom) {
                        cachedSecureRandom.setSeed(seed);
                    }
                }
            } catch (SecurityException e) {
                runnable.run();
                generateAlternativeSeed();
            }

        } catch (Exception e) {
            warn("SecureRandom", e);
            cachedSecureRandom = new SecureRandom();
        }
        return cachedSecureRandom;
    }

    public static byte[] generateAlternativeSeed() {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);

            out.writeLong(System.currentTimeMillis());
            out.writeLong(System.nanoTime());

            out.writeInt(new Object().hashCode());
            Runtime runtime = Runtime.getRuntime();
            out.writeLong(runtime.freeMemory());
            out.writeLong(runtime.maxMemory());
            out.writeLong(runtime.totalMemory());

            try {
                String s = System.getProperties().toString();
                out.writeInt(s.length());
                out.write(s.getBytes("UTF-8"));
            } catch (Exception e) {
                warn("generateAlternativeSeed", e);
            }

            try {
                Class<?> inetAddressClass = Class.forName("java.net.InetAddress");
                Object localHost = inetAddressClass.getMethod("getLocalHost").invoke(null);
                String hostName = inetAddressClass.getMethod("getHostName").invoke(localHost).toString();
                out.writeUTF(hostName);
                Object[] list = (Object[]) inetAddressClass.getMethod("getAllByName", String.class)
                        .invoke(null, hostName);
                Method getAddress = inetAddressClass.getMethod("getAddress");
                for (Object o : list) {
                    out.write((byte[]) getAddress.invoke(o));
                }
            } catch (Throwable e) {
            }

            for (int j = 0; j < 16; j++) {
                int i = 0;
                long end = System.currentTimeMillis();
                while (end == System.currentTimeMillis()) {
                    i++;
                }
                out.writeInt(i);
            }

            out.close();
            return bout.toByteArray();
        } catch (IOException e) {
            warn("generateAlternativeSeed", e);
            return new byte[1];
        }
    }

    static void warn(String s, Throwable t) {
        // not a fatal problem, but maybe reduced security
        System.out.println("Warning: " + s);
        if (t != null) {
            t.printStackTrace();
        }
    }

    public static int nextPowerOf2(int x) {
        long i = 1;
        while (i < x && i < (Integer.MAX_VALUE / 2)) {
            i += i;
        }
        return (int) i;
    }

    public static int convertLongToInt(long l) {
        if (l <= Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        } else if (l >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) l;
        }
    }

    public static int compareInt(int a, int b) {
        return a == b ? 0 : a < b ? -1 : 1;
    }

    public static int compareLong(long a, long b) {
        return a == b ? 0 : a < b ? -1 : 1;
    }

    public static long secureRandomLong() {
        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            return sr.nextLong();
        }
    }

    public static void randomBytes(byte[] bytes) {
        RANDOM.nextBytes(bytes);
    }

    public static byte[] secureRandomBytes(int len) {
        if (len <= 0) {
            len = 1;
        }
        byte[] buff = new byte[len];
        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            sr.nextBytes(buff);
        }
        return buff;
    }

    public static int randomInt(int lowerThan) {
        return RANDOM.nextInt(lowerThan);
    }

    public static int secureRandomInt(int lowerThan) {
        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            return sr.nextInt(lowerThan);
        }
    }
}
