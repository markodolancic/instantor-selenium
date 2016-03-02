package org.openqa.grid.internal;

import net.jcip.annotations.ThreadSafe;
import org.openqa.grid.common.exception.CapabilityNotPresentOnTheGridException;
import org.openqa.grid.common.exception.GridException;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.util.*;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A set of RemoteProxies.
 */
@ThreadSafe
public class ProxySet implements Iterable<RemoteProxy> {
    private static final Logger log = Logger.getLogger(ProxySet.class.getName());

    private final StampedLock stampedLock = new StampedLock();
    private final Set<RemoteProxy> proxies = new HashSet<>();

    private volatile boolean throwOnCapabilityNotPresent = true;

    public ProxySet(boolean throwOnCapabilityNotPresent) {
        this.throwOnCapabilityNotPresent = throwOnCapabilityNotPresent;
    }

    /**
     * killing the timeout detection threads.
     */
    public void teardown() {
        long stamp = stampedLock.readLock();
        try {
            proxies.forEach(RemoteProxy::teardown);
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    public boolean hasCapability(Map<String, Object> requestedCapability) {
        long stamp = stampedLock.tryOptimisticRead();
        boolean rv = false;

        for (RemoteProxy proxy : proxies) {
            if (proxy.hasCapability(requestedCapability)) {
                rv = true;
                break;
            }
        }

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                for (RemoteProxy proxy : proxies) {
                    if (proxy.hasCapability(requestedCapability)) {
                        rv = true;
                        break;
                    }
                }
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return rv;
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
        long stamp = stampedLock.writeLock();
        try {
            for (RemoteProxy p : proxies) {
                if (p.equals(proxy)) {
                    proxies.remove(p);
                    return p;
                }
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        throw new IllegalStateException("Did not contain proxy " + proxy);
    }

    public void add(RemoteProxy proxy) {
        long stamp = stampedLock.writeLock();
        try {
            proxies.add(proxy);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    public boolean contains(RemoteProxy o) {
        long stamp = stampedLock.tryOptimisticRead();
        boolean rv = proxies.contains(o);
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                rv = proxies.contains(o);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return rv;
    }

    public List<RemoteProxy> getBusyProxies() {
        long stamp = stampedLock.tryOptimisticRead();
        List<RemoteProxy> rv = proxies.stream()
                .filter(RemoteProxy::isBusy)
                .collect(Collectors.toList());

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                rv = proxies.stream()
                        .filter(RemoteProxy::isBusy)
                        .collect(Collectors.toList());
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }

        return rv;
    }

    public RemoteProxy getProxyById(String id) {
        if (id == null)
            return null;

        RemoteProxy rv = null;
        long stamp = stampedLock.tryOptimisticRead();
        for (RemoteProxy p : proxies) {
            if (id.equals(p.getId())) {
                rv = p;
                break;
            }
        }

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                for (RemoteProxy p : proxies) {
                    if (id.equals(p.getId())) {
                        rv = p;
                        break;
                    }
                }
            } finally {
                stampedLock.unlockRead(stamp);
            }

        }
        return rv;
    }


    public boolean isEmpty() {
        long stamp = stampedLock.tryOptimisticRead();
        boolean rv = proxies.isEmpty();
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                rv = proxies.isEmpty();
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return rv;
    }

    public List<RemoteProxy> getSorted() {
        long stamp = stampedLock.tryOptimisticRead();
        List<RemoteProxy> sorted = new ArrayList<>(proxies);

        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                sorted = new ArrayList<>(proxies);
            } finally {
                stampedLock.unlockRead(stamp);
            }
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
        long stamp = stampedLock.tryReadLock();
        Iterator<RemoteProxy> rv = proxies.iterator();
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                rv = proxies.iterator();
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return rv;
    }

    public int size() {
        long stamp = stampedLock.tryReadLock();
        int rv = proxies.size();
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                rv = proxies.size();
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return rv;
    }

    public void verifyAbilityToHandleDesiredCapabilities(Map<String, Object> desiredCapabilities) {
        if (isEmpty()) {
            if (throwOnCapabilityNotPresent) {
                throw new GridException("Empty pool of VM for setup " + new DesiredCapabilities(desiredCapabilities));
            } else {
                log.warning("Empty pool of nodes.");
            }
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
