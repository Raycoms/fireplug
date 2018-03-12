/**
 * This package includes the fundamental parts of the code.
 * This AbstractRecoverable which defines the behavior of all replicas.
 * The child class LocalClusterSlave which contains code only important on the local clusters.
 * The child class GlobalClusterSlave which contains code only relevant on the global cluster.
 * The ServerWrapper which can consist of a global, local or both.
 * The conflict handler which detect DB conflicts
 * The CleanServer which is a clean server without any BftSmart.
 */
package main.java.com.bag.server;