package org.openqa.grid.internal;

import net.jcip.annotations.ThreadSafe;
import org.openqa.grid.common.exception.CapabilityNotPresentOnTheGridException;
import org.openqa.grid.common.exception.GridException;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A set of RemoteProxies.
 * <p/>
 * Obeys the iteration guarantees of CopyOnWriteArraySet
 */
@ThreadSafe
public class ProxySet implements Iterable<RemoteProxy> {
    private final ReentrantReadWriteLock RW_LOCK = new ReentrantReadWriteLock(true);
    private final ReentrantReadWriteLock.ReadLock R_LOCK = RW_LOCK.readLock();
    private final ReentrantReadWriteLock.WriteLock W_LOCK = RW_LOCK.writeLock();

    private final Set<RemoteProxy> proxies = new HashSet<>();

    private static final Logger log = Logger.getLogger(ProxySet.class.getName());
    private volatile boolean throwOnCapabilityNotPresent = true;

    public ProxySet(boolean throwOnCapabilityNotPresent) {
        this.throwOnCapabilityNotPresent = throwOnCapabilityNotPresent;
    }

    /**
     * killing the timeout detection threads.
     */
    public void teardown() {
        R_LOCK.lock();
        try {
            proxies.forEach(RemoteProxy::teardown);
        } finally {
            R_LOCK.unlock();
        }
    }

    public boolean hasCapability(Map<String, Object> requestedCapability) {
        R_LOCK.lock();
        try {
            for (RemoteProxy proxy : proxies)
                if (proxy.hasCapability(requestedCapability))
                    return true;
        } finally {
            R_LOCK.unlock();
        }
        return false;
    }

    /**
     * Removes the specified instance from the proxySet
     *
     * @param proxy The proxy to remove, must be present in this set
     * @return The instance that was removed. Not null.
     */
    public RemoteProxy remove(RemoteProxy proxy) {
        // Find the original proxy. While the supplied one is logically equivalent, it may be a fresh object with
        // an empty TestSlot list, which doesn't figure into the proxy equivalence check.  Since we want to free up
        // those test sessions, we need to operate on that original object.
        W_LOCK.lock();
        try {
            for (RemoteProxy p : proxies) {
                if (p.equals(proxy)) {
                    proxies.remove(p);
                    return p;
                }
            }
        } finally {
            W_LOCK.unlock();
        }
        throw new IllegalStateException("Did not contain proxy " + proxy);
    }

    public void add(RemoteProxy proxy) {
        W_LOCK.lock();
        try {
            proxies.add(proxy);
        } finally {
            W_LOCK.unlock();
        }
    }

    public boolean contains(RemoteProxy o) {
        R_LOCK.lock();
        try {
            return proxies.contains(o);
        } finally {
            R_LOCK.unlock();
        }
    }

    public List<RemoteProxy> getBusyProxies() {
        R_LOCK.lock();
        try {
            return proxies.stream()
                    .filter(RemoteProxy::isBusy)
                    .collect(Collectors.toList());
        } finally {
            R_LOCK.unlock();
        }
    }

    public RemoteProxy getProxyById(String id) {
        if (id == null)
            return null;

        R_LOCK.lock();
        try {
            for (RemoteProxy p : proxies) {
                if (id.equals(p.getId())) {
                    return p;
                }
            }
        } finally {
            R_LOCK.unlock();
        }
        return null;
    }


    public boolean isEmpty() {
        R_LOCK.lock();
        try {
            return proxies.isEmpty();
        } finally {
            R_LOCK.unlock();
        }
    }

    public List<RemoteProxy> getSorted() {
        List<RemoteProxy> sorted;
        R_LOCK.lock();
        try {
            sorted = new ArrayList<>(proxies);
        } finally {
            R_LOCK.unlock();
        }
        Collections.sort(sorted, proxyComparator);
        return sorted;
    }

    private Comparator<RemoteProxy> proxyComparator = (proxy1, proxy2) -> {
        double p1used = proxy1.getResourceUsageInPercent();
        double p2used = proxy2.getResourceUsageInPercent();

        if (p1used == p2used) {
            long time1lastUsed = proxy1.getLastSessionStart();
            long time2lastUsed = proxy2.getLastSessionStart();
            if (time1lastUsed == time2lastUsed) return 0;
            return time1lastUsed < time2lastUsed ? -1 : 1;
        }
        return p1used < p2used ? -1 : 1;
    };

    public TestSession getNewSession(Map<String, Object> desiredCapabilities) {
        // sort the proxies first, by default by total number of
        // test running, to avoid putting all the load of the first
        // proxies.
        List<RemoteProxy> sorted = getSorted();
        for (RemoteProxy proxy : sorted) {
            TestSession session = proxy.getNewSession(desiredCapabilities);
            if (session != null) {
                return session;
            }
        }
        return null;
    }

    public Iterator<RemoteProxy> iterator() {
        R_LOCK.lock();
        try {
            return proxies.iterator();
        } finally {
            R_LOCK.unlock();
        }
    }

    public int size() {
        R_LOCK.lock();
        try {
            return proxies.size();
        } finally {
            R_LOCK.unlock();
        }
    }

    public void verifyAbilityToHandleDesiredCapabilities(Map<String, Object> desiredCapabilities) {
        R_LOCK.lock();
        try {
            if (proxies.isEmpty()) {
                if (throwOnCapabilityNotPresent) {
                    throw new GridException("Empty pool of VM for setup " + new DesiredCapabilities(desiredCapabilities));
                } else {
                    log.warning("Empty pool of nodes.");
                }
            }
        } finally {
            R_LOCK.unlock();
        }

        if (!hasCapability(desiredCapabilities)) {
            if (throwOnCapabilityNotPresent) {
                throw new CapabilityNotPresentOnTheGridException(desiredCapabilities);
            } else {
                log.warning("grid doesn't contain " + new DesiredCapabilities(desiredCapabilities) + " at the moment.");
            }

        }
    }

    public void setThrowOnCapabilityNotPresent(boolean throwOnCapabilityNotPresent) {
        this.throwOnCapabilityNotPresent = throwOnCapabilityNotPresent;
    }

}
