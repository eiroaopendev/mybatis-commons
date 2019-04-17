package ar.com.opendevsolutions

import com.google.common.base.CaseFormat
import org.apache.ibatis.jdbc.SQL
import org.springframework.data.annotation.Transient
import org.springframework.util.ReflectionUtils
import java.lang.reflect.Field

import java.lang.reflect.Type
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Arrays


abstract class GenericQueries {
    val allColumns = "*"
    private val isLikeSentence = " like #{attribute} || '%'"
    val dualTable = "DUAL"
    val defaultFormat = "yyyy-MM-dd"

    companion object {
        fun equalCondition(attribute: String, value: String) : String {
            return "$attribute = '$value'"
        }
    }


    fun getByStringAttribute(projection: String, table: String, attributeName: String, attribute: String): String {
        return object : SQL() {
            init {
                SELECT(projection)
                FROM(table)
                WHERE(attributeName + isLikeSentence)
            }
        }.toString()
    }

    fun selectWhereEqual(projection: String, table: String, valuesAndConditions: Map<String, String>): String {
        var result = SQL().SELECT(projection).FROM(table);
        val mutableIterator = valuesAndConditions.iterator()
        for (pair in mutableIterator){
            result.WHERE(equalCondition(pair.key,pair.value))
            if(mutableIterator.hasNext()){ result.AND()}
        }
        return result.toString()
    }


    fun deleteById(table: String, idIdentifier: String): String {
        return object : SQL() {
            init {
                DELETE_FROM(table)
                WHERE(CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, idIdentifier) + "=#{$idIdentifier}")
            }
        }.toString()
    }

    fun executeFunction(function: String, vararg parameters: String): String {
        return object : SQL() {
            init {
                SELECT(function + parseFunctionParameters(*parameters))
                FROM(dualTable)
            }
        }.toString()
    }

    fun insert(tableName: String, valuesToInsert: Map<String, String>): String {
        val statement = object : SQL() {
            init {
                INSERT_INTO(tableName)
            }
        }     //VALUES("LOGIN_ID", "#{loginId}");
        valuesToInsert.forEach { key, value ->
            if (value != null && value != "null") {
                statement.VALUES(key, value)
            } else {
                statement.VALUES(key, null)
            }
        }
        return statement.toString()
    }

    fun <T : Any> insert(tableName: String, element: T): String {
        val statement = object : SQL() {
            init {
                INSERT_INTO(tableName)
            }
        }
        setDynamicInsertionFields(element.javaClass, element, statement)
        return statement.toString()
    }

    private fun <T> setDynamicInsertionFields(clazz: Class<T>, element: T, statement: SQL) {
        Arrays.asList<Field>(*clazz.fields).forEach { field ->
            if (ReflectionUtils.getField(field, element) != null && !field.isAnnotationPresent(Transient::class.java)) {
                val fieldType = field.annotatedType.type
                val jdbcType = getJDBCType(fieldType)
                statement.VALUES(
                        CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, field.name),
                        "#{" + field.name + "," + "jdbcType=" + jdbcType + "}")
            }
        }
    }

    private fun getJDBCType(fieldType: Type): String {
        var jdbcType = "VARCHAR"
        when (fieldType.typeName) {
            "java.lang.Integer" -> jdbcType = "INTEGER"
            "java.lang.Double" -> jdbcType = "INTEGER"
            "java.util.Date" -> jdbcType = "DATE"
            else -> {
            }
        }
        return jdbcType
    }

    internal fun parseFunctionParameters(vararg parameters: String): String {
        val parsedParameters = StringBuilder("(")
        for (i in parameters.indices) {
            if (i == 0) {
                parsedParameters.append(parameters[i])
            } else {
                parsedParameters.append(" , ").append(parameters[i])
            }
        }
        parsedParameters.append(" )")
        return parsedParameters.toString()
    }

    fun insertFromMap(tablaDomicilio: String,
                      valuesToInsert: MutableMap<String, Any>,
                      customFieldMapping: Map<String, String>,
                      ignoredFields: List<String>): String {
        return insertFromMap(tablaDomicilio, valuesToInsert, customFieldMapping, ignoredFields, defaultFormat)
    }

    fun insertFromMap(tablaDomicilio: String,
                      valuesToInsert: MutableMap<String, Any>,
                      customFieldMapping: Map<String, String>,
                      ignoredFields: List<String>,
                      dateFormat: String): String {
        val statement = object : SQL() {
            init {
                INSERT_INTO(tablaDomicilio)
            }
        }
        valuesToInsert.forEach { key, value ->
            if (value != null && value != "null" && !ignoredFields.contains(key)) {
                val determinedType = findJdbcType(value, key, customFieldMapping)
                forceObjectConversionFromMap(key, valuesToInsert, determinedType, dateFormat)
                statement.VALUES(CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, key),
                        "#{$key,jdbcType=$determinedType}")
            }
        }

        return statement.toString()
    }

    private fun findJdbcType(value: Any, key: String, customFieldMapping: Map<String, String>): String {
        return customFieldMapping.getOrDefault(key, getJDBCType(value.javaClass.annotatedSuperclass.type))
    }

    private fun forceObjectConversionFromMap(key: String, valuesToInsert: MutableMap<String, Any>, determinedType: String, dateFormat: String?) {
        when (determinedType) {
            "DATE" ->
                //Force Value conversion to Date
                try {
                    valuesToInsert.replace(key, SimpleDateFormat(dateFormat
                            ?: defaultFormat).parse(valuesToInsert[key] as String))
                } catch (e: ParseException) {
                    e.printStackTrace()
                }

            else -> {
            }
        }
    }

    private fun forceObjectConversionFromMap(key: String, valuesToInsert: MutableMap<String, Any>, determinedType: String) {
        forceObjectConversionFromMap(key, valuesToInsert, determinedType, defaultFormat)
    }
}
