package ar.edu.itba.pod.legajo50758.message;

import java.io.Serializable;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public enum Operation implements Serializable {

	ADD, QUERY, MOVE, MOVED, NODEUP, NODEDOWN
}
