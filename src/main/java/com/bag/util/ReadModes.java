package com.bag.util;

/**
 * Possible Client readmodes.
 */
public enum ReadModes
{
    PESSIMISTIC,
    LOCALLY_UNORDERED,
    GLOBALLY_UNORDERED,
    TO_F_PLUS_1_LOCALLY,
    TO_F_PLUS_1_GLOBALLY,
    TO_1_OTHER,
    UNSAFE
}
