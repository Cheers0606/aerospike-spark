package com.psbc.spark.aerospike

import com.psbc.spark.aerospike.AqlParser._
import org.scalatest.{FlatSpec, Matchers}


class AqlParserSpec extends FlatSpec with Matchers {

  behavior of "remove double spaces"

  it should "remove duplicates spaces from string" in {
    val hasDuplicateSpaces: String = "a  string  with  double  spaces"
    val result = removeDoubleSpaces(hasDuplicateSpaces)

    result shouldEqual "a string with double spaces"
  }

  it should "remove all double spaces from string" in {
    val hasDuplicateSpaces: String = "a    string    with    double  spaces"
    val result = removeDoubleSpaces(hasDuplicateSpaces)

    result shouldEqual "a string with double spaces"
  }

  it should "return string when has not double spaces" in {
    val cleanString: String = "this is a clean string"
    removeDoubleSpaces(cleanString) shouldEqual cleanString
  }

  behavior of "calculate ranges"

  it should "with even partition" in {
    val ranges = calculateRanges(0, 100, 2)
    ranges shouldEqual Vector((0, 49), (50, 100))
  }
  it should "with odd partition" in {
    val ranges = calculateRanges(0, 100, 3)
    ranges shouldEqual Vector((0, 32), (33, 65), (66, 100))
  }

  behavior of "parse aql query"

  it should "throw Exception when missing SELECT from query" in {
    intercept[IllegalArgumentException] {
      parseSelect("query without SEL.ECT", 1)
    }
  }

  it should "throw Exception when missing equals or between on WHERE query" in {
    val aNamespace: String = "aNamespace"
    val aSet: String = "aSet"
    val filteredBin: String = "aBin"
    val aqlQuery: String = "SELECT bin1, bin2, bin3 FROM " + "%s.%s".format(aNamespace, aSet) +
      " WHERE " + filteredBin + " nonEquals filtered"

    intercept[IllegalArgumentException] {
     parseSelect(aqlQuery)
    }
  }

  it should "throw Exception when partition less than 1" in {
    val aNamespace: String = "aNamespace"
    val aSet: String = "aSet"
    val aqlQuery: String = "SELECT bin1, bin2, bin3 FROM " + "%s.%s".format(aNamespace, aSet)

    intercept[IllegalArgumentException] {
      parseSelect(aqlQuery, 0)
    }
  }

  it should "throw Exception when query is empty" in {
    intercept[IllegalArgumentException] {
      parseSelect("")
    }
  }

  it should "throw Exception when query is null" in {
    intercept[IllegalArgumentException] {
      parseSelect(null)
    }
  }

  it should "extract values" in {
    val aNamespace: String = "aNamespace"
    val aSet: String = "aSet"
    val aqlQuery: String = "SELECT bin1, bin2, bin3 FROM " + "%s.%s".format(aNamespace, aSet)

    val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = parseSelect(aqlQuery).toArray()

    namespace shouldEqual aNamespace
    set shouldEqual aSet
    bins shouldEqual Array("bin1", "bin2", "bin3")
    filterType should be(0)
    filterVals shouldEqual Seq((0, 0))
  }


  it should "extract values when using a String filter" in {
    val aNamespace: String = "aNamespace"
    val aSet: String = "aSet"
    val filteredBin: String = "aBin"
    val aFilter: String = "aFilter"
    val aqlQuery: String = "SELECT bin1, bin2, bin3 FROM " + "%s.%s".format(aNamespace, aSet) +
      " WHERE " + filteredBin + " = " + aFilter

    val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = parseSelect(aqlQuery).toArray()

    namespace shouldEqual aNamespace
    set shouldEqual aSet
    bins shouldEqual Array("bin1", "bin2", "bin3")
    filterType should be(1)
    filterBin shouldEqual filteredBin
    filterVals shouldEqual Seq((0, 0))
    filterStringVal shouldEqual aFilter
  }

  it should "extract values when using a numeric filter" in {
    val aNamespace: String = "aNamespace"
    val aSet: String = "aSet"
    val filteredBin: String = "aBin"
    val aFilter: Int = 1
    val aqlQuery: String = "SELECT bin1, bin2, bin3 FROM " + "%s.%s".format(aNamespace, aSet) +
      " WHERE " + filteredBin + " = " + aFilter

    val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = parseSelect(aqlQuery).toArray()

    namespace shouldEqual aNamespace
    set shouldEqual aSet
    bins shouldEqual Array("bin1", "bin2", "bin3")
    filterType should be(2)
    filterBin shouldEqual filteredBin
    filterVals shouldEqual Seq((aFilter, 0))
  }

  it should "extract values from when using a range with even partition" in {
    val aNamespace: String = "aNamespace"
    val aSet: String = "aSet"
    val filteredBin: String = "aBin"

    val minValue: Int = 0
    val maxValue: Int = 100
    val aqlQuery: String = "SELECT bin1, bin2, bin3 FROM " + "%s.%s".format(aNamespace, aSet) +
      " WHERE " + filteredBin + " BETWEEN " + minValue + " AND " + maxValue

    val partition: Int = 2

    val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = parseSelect(aqlQuery, partition).toArray()

    namespace shouldEqual aNamespace
    set shouldEqual aSet
    bins shouldEqual Array("bin1", "bin2", "bin3")
    filterType should be(3)
    filterBin shouldEqual filteredBin
    val middle: Int = (maxValue - minValue) / partition;
    filterVals shouldEqual Vector((minValue, middle - 1), (middle, maxValue))

    println("filterVals: " + filterVals);
  }


}
