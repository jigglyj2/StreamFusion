/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import java.util.List;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Base64Encode;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Hexadecimal;
import tech.streamfusion.proto.plan.v1.Md5;
import tech.streamfusion.proto.plan.v1.Sha1;
import tech.streamfusion.proto.plan.v1.Sha2Dynamic;
import tech.streamfusion.proto.plan.v1.ShaAlgorithm;
import tech.streamfusion.proto.plan.v1.ShaDigest;
import tech.streamfusion.proto.plan.v1.Unhex;

/** Translates byte-oriented scalar functions into native expressions. */
final class StreamFusionBinaryFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionBinaryFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if ("FROM_BASE64".equals(function)) {
            return "FROM_BASE64 stays on Flink because decoded bytes are not guaranteed to satisfy Arrow's UTF-8 invariant for the declared VARCHAR result";
        }
        if ("SHA2".equals(function)) {
            return "SHA2 stays on Flink when a non-null literal digest length is not one of 224, 256, 384, and 512 so Flink preserves initialization-time failure";
        }
        if (shaAlgorithm(function) != null) {
            return function + " input or result type is not parity-approved";
        }
        return null;
    }

    static Expression hexadecimal(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"HEX".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || !supportsHex(operandType.getTypeRoot())) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setHexadecimal(Hexadecimal.newBuilder().setOperand(operand))
                        .build();
    }

    static Expression base64Encode(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"TO_BASE64".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || !supportsBase64(operandType.getTypeRoot())) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setBase64Encode(Base64Encode.newBuilder().setOperand(operand))
                        .build();
    }

    static Expression unhex(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"UNHEX".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARBINARY) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setUnhex(Unhex.newBuilder().setOperand(operand))
                        .build();
    }

    static Expression md5(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"MD5".equals(functionName(expression))
                || (expectedType.getTypeRoot() != LogicalTypeRoot.CHAR
                        && expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR)) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || !supportsMd5(operandType.getTypeRoot())) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setMd5(Md5.newBuilder().setOperand(operand))
                        .build();
    }

    static Expression fixedSha(Object expression, RowType inputType, LogicalType expectedType) {
        ShaAlgorithm algorithm = shaAlgorithm(functionName(expression));
        if (algorithm == null
                || (expectedType.getTypeRoot() != LogicalTypeRoot.CHAR
                        && expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR)) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || !supportsMd5(operandType.getTypeRoot())) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setShaDigest(ShaDigest.newBuilder().setOperand(operand).setAlgorithm(algorithm))
                        .build();
    }

    static Expression sha1(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"SHA1".equals(functionName(expression))
                || (expectedType.getTypeRoot() != LogicalTypeRoot.CHAR
                        && expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR)) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setSha1(Sha1.newBuilder().setOperand(operand))
                        .build();
    }

    static Expression sha2Literal(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"SHA2".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        Integer bitLength = integerLiteral(operands.get(1));
        ShaAlgorithm algorithm = sha2Algorithm(bitLength);
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (algorithm == null || operandType == null || operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setShaDigest(ShaDigest.newBuilder().setOperand(operand).setAlgorithm(algorithm))
                        .build();
    }

    static Expression sha2Dynamic(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"SHA2".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2 || integerLiteral(operands.get(1)) != null) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        LogicalType bitLengthType = logicalType(operands.get(1), inputType);
        if (operandType == null
                || operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                || bitLengthType == null
                || bitLengthType.getTypeRoot() != LogicalTypeRoot.INTEGER) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        Expression bitLength =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, bitLengthType);
        return operand == null || bitLength == null
                ? null
                : Expression.newBuilder()
                        .setSha2Dynamic(
                                Sha2Dynamic.newBuilder().setOperand(operand).setBitLength(bitLength))
                        .build();
    }

    private static boolean supportsHex(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.VARCHAR;
    }

    private static boolean supportsBase64(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.VARCHAR || type == LogicalTypeRoot.BINARY || type == LogicalTypeRoot.VARBINARY;
    }

    private static boolean supportsMd5(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.VARCHAR || type == LogicalTypeRoot.BINARY || type == LogicalTypeRoot.VARBINARY;
    }

    private static ShaAlgorithm shaAlgorithm(String function) {
        if ("SHA224".equals(function)) {
            return ShaAlgorithm.SHA_ALGORITHM_224;
        }
        if ("SHA256".equals(function)) {
            return ShaAlgorithm.SHA_ALGORITHM_256;
        }
        if ("SHA384".equals(function)) {
            return ShaAlgorithm.SHA_ALGORITHM_384;
        }
        if ("SHA512".equals(function)) {
            return ShaAlgorithm.SHA_ALGORITHM_512;
        }
        return null;
    }

    private static ShaAlgorithm sha2Algorithm(Integer bitLength) {
        if (bitLength == null) {
            return null;
        }
        switch (bitLength) {
            case 224:
                return ShaAlgorithm.SHA_ALGORITHM_224;
            case 256:
                return ShaAlgorithm.SHA_ALGORITHM_256;
            case 384:
                return ShaAlgorithm.SHA_ALGORITHM_384;
            case 512:
                return ShaAlgorithm.SHA_ALGORITHM_512;
            default:
                return null;
        }
    }
}
