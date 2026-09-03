{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyP5+zjXp3h5pjKlBrFeI+iT",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/abhishekganganalli/pyspark_codes/blob/main/pyspark_assign_1.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "bXySLSmF5u9K",
        "outputId": "ffa9c753-7e38-48d9-f624-43c9108bbe8d"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[10, 20, 30, 40, 50, 60]\n"
          ]
        }
      ],
      "source": [
        "#Q1\n",
        "#Dataset\n",
        "#[5, 10, 15, 20, 25, 30]\n",
        "#Problem:\n",
        "#Create an RDD from the dataset and multiply every number by 2\n",
        "\n",
        "from pyspark.sql import SparkSession\n",
        "spark=SparkSession.builder.appName(\"sample_rdd\").getOrCreate()\n",
        "number=[5,10,15,20,25,30]\n",
        "\n",
        "\n",
        "sc = spark.sparkContext\n",
        "\n",
        "rdd=sc.parallelize(number)\n",
        "\n",
        "result=rdd.map(lambda x:x*2)\n",
        "print(result.collect())"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "#Q2\n",
        "#Dataset\n",
        "#[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]\n",
        "#Problem:\n",
        "#Find all numbers greater than 5."
      ],
      "metadata": {
        "id": "m60knK2f9VWm"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql import SparkSession\n",
        "spark=SparkSession.builder.appName(\"sample-rdd_2\").getOrCreate()\n",
        "numbers=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]\n",
        "sc=spark.sparkContext\n",
        "\n",
        "rdd=sc.parallelize(numbers)\n",
        "result=rdd.filter(lambda x:x > 5)\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "Bd7PkrfB9e6G",
        "outputId": "3ae205e6-b109-4553-b917-8670c2ebf166"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[6, 7, 8, 9, 10]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# #Q\n",
        "# Dataset\n",
        "# [10, 20, 30, 40, 50]\n",
        "# Problem:\n",
        "# Add 100 to every number.\n",
        "# Expected Output\n",
        "# [110, 120, 130, 140, 150] '''\n",
        "\n",
        "from pyspark.sql import SparkSession\n",
        "spark=SparkSession.builder.appName(\"sample_rdd\").getOrCreate()\n",
        "n=[10, 20, 30, 40, 50]\n",
        "sc=spark.sparkContext\n",
        "rdd=sc.parallelize(n)\n",
        "result=rdd.map(lambda x:x+100)\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "gqQLoidG_HwX",
        "outputId": "7fd816d7-4b6a-4b7f-ed4f-3a0e08107e4c"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[110, 120, 130, 140, 150]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q4\n",
        "# Dataset\n",
        "# [2, 4, 6, 8, 10]\n",
        "# Problem:\n",
        "# Find the square of every number.\n",
        "\n",
        "from pyspark.sql import SparkSession\n",
        "spark=SparkSession.builder.appName(\"sample_rdd\").getOrCreate()\n",
        "square_n=[2, 4, 6, 8, 10]\n",
        "sc=spark.sparkContext\n",
        "rdd=sc.parallelize(square_n)\n",
        "result=rdd.map(lambda x:x**2)\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "wn7pkXr-Afi9",
        "outputId": "e83c8dc3-dc70-4a68-b0b5-de0549ca55d2"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[4, 16, 36, 64, 100]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q5\n",
        "# Dataset\n",
        "# [10, 15, 20, 25, 30, 35, 40]\n",
        "# Problem:\n",
        "# Find the count of numbers greater than 20\n",
        "number=[10, 15, 20, 25, 30, 35, 40]\n",
        "sc=spark.sparkContext\n",
        "rdd=sc.parallelize(number)\n",
        "result=rdd.filter(lambda x:x>=20).count()\n",
        "print(result)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "tER0SsLHCDvo",
        "outputId": "68921248-551f-42b5-95fd-1ea3e1c0c587"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "5\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "# Q6\n",
        "# Dataset\n",
        "# [10, 20, 30, 40, 50]\n",
        "# Problem:\n",
        "# Calculate the sum of all numbers.\n",
        "\n",
        "sum= [10, 20, 30, 40, 50]\n",
        "sc=spark.sparkContext\n",
        "rdd=sc.parallelize(sum)\n",
        "result=rdd.reduce(lambda a,b:a+b)\n",
        "print(result)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "jCdilypeD1jK",
        "outputId": "c47bce12-ebe9-4a59-9abb-838b7eb4255e"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "150\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q7\n",
        "# Dataset\n",
        "# [10, 50, 20, 90, 40, 70]\n",
        "# Problem:\n",
        "# Find the maximum value\n",
        "max_num= [10, 50, 20, 90, 40, 70]\n",
        "sc=spark.sparkContext\n",
        "rdd=sc.parallelize(max_num)\n",
        "result=rdd.max()\n",
        "print(result)\n"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "s3zNsTFvE0-A",
        "outputId": "acdb7765-a587-4fe6-80a3-113b7d556c8e"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "90\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q8\n",
        "# Dataset\n",
        "# [10, 50, 20, 90, 40, 70]\n",
        "# Problem:\n",
        "# Find the minimum value.\n",
        "min_num= [10, 50, 20, 90, 40, 70]\n",
        "sc=spark.sparkContext\n",
        "rdd=sc.parallelize(min_num)\n",
        "result=rdd.min()\n",
        "print(result)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "IiEROw6EGK_Z",
        "outputId": "2a167b33-57cb-4554-d353-2b92d84730e1"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "10\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q9\n",
        "# Dataset\n",
        "# [1, 2, 3, 4, 5]\n",
        "# Problem:\n",
        "# Calculate the product of all numbers.\n",
        "\n",
        "product_num=[1,2,3,4,5]\n",
        "sc=spark.sparkContext\n",
        "rdd=sc.parallelize(product_num)\n",
        "result=rdd.reduce(lambda x,y:x*y)\n",
        "print(result)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "p6K-kcBiGfVY",
        "outputId": "f1c09f5f-4c5d-45a6-b5b2-2a53d5359cd2"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "120\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q10\n",
        "# Dataset\n",
        "# [10, 20, 30, 40, 50]\n",
        "# Problem:\n",
        "# Calculate the total after adding an initial value of 100.\n",
        "\n",
        "initial_value=[10,20,30,40,50]\n",
        "sc=spark.sparkContext\n",
        "rdd=sc.parallelize(initial_value)\n",
        "result=rdd.reduce(lambda x,y:x+y)+100\n",
        "print(result)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "qNOyOwWPHViI",
        "outputId": "d571a07a-9e83-4333-b5b4-43e4bf22b98f"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "250\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "data=[\n",
        " (101, \"Alice\", \"IT\", 80000),\n",
        " (102, \"Bob\", \"HR\", 60000),\n",
        " (103, \"Charlie\", \"IT\", 90000),\n",
        " (104, \"David\", \"Finance\", 75000),\n",
        " (105, \"Eva\", \"HR\", 65000),\n",
        " (106, \"Frank\", \"Finance\", 85000),\n",
        " (107, \"Grace\", \"IT\", 95000),\n",
        " (108, \"Henry\", \"Sales\", 70000),\n",
        " (109, \"Ivy\", \"Sales\", 72000),\n",
        " (110, \"Jack\", \"IT\", 88000)\n",
        "]"
      ],
      "metadata": {
        "id": "2HYrJxSjIzRg"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Q11\n",
        "# Problem:\n",
        "# Find all employees who work in the IT department.\n",
        "rdd=sc.parallelize(data)\n",
        "result=rdd.filter(lambda x:x[2]==\"IT\").map(lambda x:x[1])\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "HAw_noHzI8UB",
        "outputId": "b53abb45-2271-4875-95b8-3882cb83c173"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "['Alice', 'Charlie', 'Grace', 'Jack']\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q12\n",
        "# Problem:\n",
        "# Find employees earning more than 80000\n",
        "\n",
        "rdd=sc.parallelize(data)\n",
        "result=rdd.filter(lambda x:x[3]>80000).map(lambda x:x[1])\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "n0PQ2629cMjE",
        "outputId": "9ced3b5f-37ad-48fd-b01a-8150c772d0ca"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "['Charlie', 'Frank', 'Grace', 'Jack']\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q13\n",
        "# Problem:\n",
        "# Find the names and salaries of all employees.\n",
        "rdd=sc.parallelize(data)\n",
        "result=rdd.map(lambda x:(x[1],x[3]))\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "_egDrToFdKJk",
        "outputId": "9bd1a22b-f926-437b-aa73-1ac604aa06a6"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('Alice', 80000), ('Bob', 60000), ('Charlie', 90000), ('David', 75000), ('Eva', 65000), ('Frank', 85000), ('Grace', 95000), ('Henry', 70000), ('Ivy', 72000), ('Jack', 88000)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q14\n",
        "# Problem:\n",
        "# Increase every employee's salary by 10%.\n",
        "rdd=sc.parallelize(data)\n",
        "result=rdd.map(lambda x:(x[1],x[3]+x[3]*10/100))\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "YSN744iSegXI",
        "outputId": "10d23a61-fb3f-437b-c7a8-b41c01ab0a01"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('Alice', 88000.0), ('Bob', 66000.0), ('Charlie', 99000.0), ('David', 82500.0), ('Eva', 71500.0), ('Frank', 93500.0), ('Grace', 104500.0), ('Henry', 77000.0), ('Ivy', 79200.0), ('Jack', 96800.0)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q15\n",
        "# Problem:\n",
        "# Find the names of employees earning between 70000 and 90000 inclusive.\n",
        "rdd=sc.parallelize(data)\n",
        "result=rdd.filter(lambda x : x[3] >= 70000 and x[3] <=90000).map(lambda x:x[1])\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "QHhWdSlQfRPH",
        "outputId": "a310d6ac-c981-471f-8ab9-00cb472f37ed"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "['Alice', 'Charlie', 'David', 'Frank', 'Henry', 'Ivy', 'Jack']\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q16\n",
        "# Problem:\n",
        "# Find IT employees whose salary is greater than 85000.\n",
        "rdd=sc.parallelize(data)\n",
        "result=rdd.filter(lambda x: x[3]>85000 and x[2]==\"IT\").map(lambda x: x[1])\n",
        "print(result.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "L_PpUkhXgiuz",
        "outputId": "9bc666c0-c79a-4296-f4e2-7e9c2816c387"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "['Charlie', 'Grace', 'Jack']\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q17\n",
        "# Problem:\n",
        "# Find the total number of employees.\n",
        "rdd=sc.parallelize(data)\n",
        "result=rdd.count()\n",
        "print(result)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "hab126O_hj2X",
        "outputId": "69200df2-abe0-4eba-8290-aa0db8675184"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "10\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "dataset=[\n",
        " \"spark is fast\",\n",
        " \"spark is powerful\",\n",
        " \"hadoop is distributed\",\n",
        " \"spark and hadoop are popular\",\n",
        " \"kafka is streaming\"\n",
        "]\n"
      ],
      "metadata": {
        "id": "U3-1GckUiLDp"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Problem:\n",
        "# Convert all sentences into individual words.\n",
        "rdd=sc.parallelize(dataset)\n",
        "result=rdd.flatMap(lambda x:x.split())\n",
        "for word in result.collect():\n",
        "  print(word)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "wAGmCNfEiPxS",
        "outputId": "f54e5b96-59c3-4051-cd82-98a665e38f43"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "spark\n",
            "is\n",
            "fast\n",
            "spark\n",
            "is\n",
            "powerful\n",
            "hadoop\n",
            "is\n",
            "distributed\n",
            "spark\n",
            "and\n",
            "hadoop\n",
            "are\n",
            "popular\n",
            "kafka\n",
            "is\n",
            "streaming\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q19\n",
        "# Problem:\n",
        "# Find the total number of words.\n",
        "rdd=sc.parallelize(dataset)\n",
        "result=rdd.flatMap(lambda x:x.split(' '))\n",
        "print(result.count())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "GWkitcmdjoXF",
        "outputId": "9b090371-f308-4f2f-c045-314466ea7516"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "17\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q20\n",
        "# Problem:\n",
        "# Find all words having more than 4 characters.\n",
        "rdd=sc.parallelize(dataset)\n",
        "word=rdd.flatMap(lambda x:x.split(' '))\n",
        "result=word.filter(lambda w: len(w)>4)\n",
        "for word in result.collect():\n",
        "  print(word)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "gBQYNak4kce2",
        "outputId": "3a4ae2fd-ec8c-41a7-a567-ea0c6d5fa180"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "spark\n",
            "spark\n",
            "powerful\n",
            "hadoop\n",
            "distributed\n",
            "spark\n",
            "hadoop\n",
            "popular\n",
            "kafka\n",
            "streaming\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q21\n",
        "# Problem:\n",
        "# Convert every word into uppercase.\n",
        "rdd= sc.parallelize(dataset)\n",
        "word=rdd.flatMap(lambda x: x.split(\" \"))\n",
        "result=word.map(lambda w: w.upper())\n",
        "for upper in result.collect():\n",
        "  print(upper)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "NXyjZJm4m_YM",
        "outputId": "2835d295-b6cb-4c41-8674-80aa088a6f25"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "SPARK\n",
            "IS\n",
            "FAST\n",
            "SPARK\n",
            "IS\n",
            "POWERFUL\n",
            "HADOOP\n",
            "IS\n",
            "DISTRIBUTED\n",
            "SPARK\n",
            "AND\n",
            "HADOOP\n",
            "ARE\n",
            "POPULAR\n",
            "KAFKA\n",
            "IS\n",
            "STREAMING\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q22\n",
        "# Problem:\n",
        "# Find the number of unique words.\n",
        "rdd=sc.parallelize(dataset)\n",
        "word=rdd.flatMap(lambda x:x.split(' '))\n",
        "result=word.distinct().count()\n",
        "print(result)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "MR1khaW5nxst",
        "outputId": "1077f710-c8bf-4e1e-abdd-d3ff38649d0a"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "11\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "word=[\n",
        " \"spark is fast\",\n",
        " \"spark is powerful\",\n",
        " \"spark is easy\",\n",
        " \"hadoop is distributed\",\n",
        " \"spark is fast\"\n",
        "]"
      ],
      "metadata": {
        "id": "J5q8lUCfpNUl"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Q23\n",
        "# Problem:\n",
        "# Calculate the frequency of every word.\n",
        "rdd=sc.parallelize(word)\n",
        "rdd1=rdd.flatMap(lambda x:x.split(\" \"))\n",
        "map_rdd=rdd1.groupBy(lambda word:word)\n",
        "count_rdd=map_rdd.mapValues(len)\n",
        "for w,c in count_rdd.collect():\n",
        "  print({f\"{w} {c}\"})\n",
        "\n"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "WfOM6MpyqChA",
        "outputId": "4ed55708-db87-4b83-cae4-9a8352d6b52c"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "{'fast 2'}\n",
            "{'powerful 1'}\n",
            "{'easy 1'}\n",
            "{'hadoop 1'}\n",
            "{'distributed 1'}\n",
            "{'spark 4'}\n",
            "{'is 5'}\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q24\n",
        "# Problem:\n",
        "# Find the most frequently occurring word.\n",
        "# Expected Output\n",
        "rdd=sc.parallelize(word)\n",
        "rdd1=rdd.flatMap(lambda x: x.split(\" \"))\n",
        "map_rdd=rdd1.map(lambda x: (x,1))\n",
        "freq_count=map_rdd.reduceByKey(lambda a,b: a+b)\n",
        "most_frequent=freq_count.sortBy(lambda x:x[1], ascending=False).first()\n",
        "print(f\"{most_frequent[0],{most_frequent[1]}}\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "GTXdDf8zri2a",
        "outputId": "d5040bbf-11b7-41ad-af2f-7a4126fd1ced"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "('is', {5})\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q25\n",
        "# Problem:\n",
        "# Find all words that occur more than once.\n",
        "rdd=sc.parallelize(word)\n",
        "rdd1=rdd.flatMap(lambda x: x.split(\" \"))\n",
        "map_rdd=rdd1.map(lambda x: (x,1))\n",
        "word_counts=map_rdd.reduceByKey(lambda a,b: a+b)\n",
        "result_rdd = word_counts.filter(lambda x: x[1] > 1)\n",
        "for w,c in result_rdd.collect():\n",
        "  print(f\"{w},{c}\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "skvf0fCSumK7",
        "outputId": "8ddc2546-0622-4073-d622-4f46432af5e4"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "fast,2\n",
            "spark,4\n",
            "is,5\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q26\n",
        "# Problem:\n",
        "# Calculate the total number of words.\n",
        "rdd=sc.parallelize(word)\n",
        "rdd1=rdd.flatMap(lambda x:x.split(\" \"))\n",
        "word_count=rdd1.count()\n",
        "print(word_count)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "Lxz3Yw6kfqDI",
        "outputId": "2961763d-a95b-45ff-d4d3-f80d295aa0cb"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "15\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "salary=[\n",
        " (\"IT\", 80000),\n",
        " (\"HR\", 60000),\n",
        " (\"IT\", 90000),\n",
        " (\"Finance\", 75000),\n",
        " (\"HR\", 65000),\n",
        " (\"Finance\", 85000),\n",
        " (\"IT\", 95000),\n",
        " (\"Sales\", 70000),\n",
        " (\"Sales\", 72000),\n",
        " (\"IT\", 88000)\n",
        "]"
      ],
      "metadata": {
        "id": "0GarhF6_jDPZ"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Q27\n",
        "# Problem:\n",
        "# Group all salaries by department.\n",
        "rdd=sc.parallelize(salary)\n",
        "grouped_salaries=rdd.groupByKey().mapValues(list)\n",
        "for salaries in grouped_salaries.collect():\n",
        "  print(salaries)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "gOnTY-mIjLXi",
        "outputId": "a676ad83-3b45-44d5-98f2-7c46168a4a04"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "('IT', [80000, 90000, 95000, 88000])\n",
            "('HR', [60000, 65000])\n",
            "('Finance', [75000, 85000])\n",
            "('Sales', [70000, 72000])\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q28\n",
        "# Problem:\n",
        "# Calculate the total salary for every department\n",
        "\n",
        "rdd=sc.parallelize(salary)\n",
        "total_salary=rdd.reduceByKey(lambda x,y:x+y)\n",
        "\n",
        "print(total_salary.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "jZkWJGA7lkCN",
        "outputId": "05b1234e-5524-4c29-c54c-80f86fce6ac7"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('IT', 353000), ('HR', 125000), ('Finance', 160000), ('Sales', 142000)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q29\n",
        "# Problem:\n",
        "# Calculate the average salary for every department.\n",
        "rdd=sc.parallelize(salary)\n",
        "avg_salary=rdd.groupByKey().mapValues(lambda salaries:sum(salaries)/len(salaries))\n",
        "print(avg_salary.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "58nTVMHdnYl2",
        "outputId": "01223d58-754b-4aa2-ca5a-d5c62c40f350"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('IT', 88250.0), ('HR', 62500.0), ('Finance', 80000.0), ('Sales', 71000.0)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q30\n",
        "# Problem:\n",
        "# Find the highest salary in every department.\n",
        "salary=[\n",
        " (\"IT\", 80000),\n",
        " (\"HR\", 60000),\n",
        " (\"IT\", 90000),\n",
        " (\"Finance\", 75000),\n",
        " (\"HR\", 65000),\n",
        " (\"Finance\", 85000),\n",
        " (\"IT\", 95000),\n",
        " (\"Sales\", 70000),\n",
        " (\"Sales\", 72000),\n",
        " (\"IT\", 88000)\n",
        "]\n",
        "rdd=sc.parallelize(salary)\n",
        "highest_salary=rdd.groupByKey().mapValues(lambda salaries:max(salaries))\n",
        "print(highest_salary.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "yiYW7xyEotl1",
        "outputId": "437d4e3b-a7b5-4c77-a277-4f95405d0488"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('IT', 95000), ('HR', 65000), ('Finance', 85000), ('Sales', 72000)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q31\n",
        "# Problem:\n",
        "# Find the department having the highest total salary\n",
        "rdd=sc.parallelize(salary)\n",
        "salary=rdd.reduceByKey(lambda x,y:x+y)\n",
        "total_salary=salary.sortBy(lambda x:x[1],ascending=False).first()\n",
        "print(total_salary)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "O1xw4B6gqj00",
        "outputId": "f7dd4be3-d3b3-49f1-b5ba-7e1d5d0c3bfb"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "('IT', 353000)\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "sales=[\n",
        " (\"Laptop\", 2, 50000),\n",
        " (\"Mobile\", 5, 20000),\n",
        " (\"Laptop\", 1, 50000),\n",
        " (\"Tablet\", 3, 15000),\n",
        " (\"Mobile\", 2, 20000),\n",
        " (\"Laptop\", 4, 50000),\n",
        " (\"Tablet\", 2, 15000),\n",
        " (\"Mobile\", 3, 20000)\n",
        "]\n"
      ],
      "metadata": {
        "id": "qmEuCfTbsst1"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Q32\n",
        "# Problem:\n",
        "# Calculate the revenue for every transaction\n",
        "rdd=sc.parallelize(sales)\n",
        "revenue_rdd=rdd.map(lambda x:(x[0],x[1]*x[2]))\n",
        "for product,revenue in revenue_rdd.collect():\n",
        "  print(f\"{product}=>{revenue}\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "H7sjwc9xsv2W",
        "outputId": "3e6ff316-52c1-42a1-b40b-930f254ccc0e"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Laptop=>100000\n",
            "Mobile=>100000\n",
            "Laptop=>50000\n",
            "Tablet=>45000\n",
            "Mobile=>40000\n",
            "Laptop=>200000\n",
            "Tablet=>30000\n",
            "Mobile=>60000\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q33\n",
        "# Problem:\n",
        "# Calculate total revenue for every product\n",
        "rdd=sc.parallelize(sales)\n",
        "revenue=rdd.map(lambda x:(x[0],x[1]*x[2]))\n",
        "product=revenue.reduceByKey(lambda x,y:x+y)\n",
        "for item,value in product.collect():\n",
        "  print(f\"{item} -> {value}\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "V6m1io3Yunuh",
        "outputId": "26beb743-2fdc-4f87-b23f-34ddb2e8ce33"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Laptop -> 350000\n",
            "Mobile -> 200000\n",
            "Tablet -> 75000\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q34\n",
        "# Problem:\n",
        "# Calculate the total quantity sold for every product\n",
        "rdd=sc.parallelize(sales)\n",
        "count=rdd.map(lambda x:(x[0],x[1]))\n",
        "total_quantity=count.reduceByKey(lambda x,y:x+y)\n",
        "for product,total in total_quantity.collect():\n",
        "  print(f'{product} -> {total}')"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "buwfaTTovowF",
        "outputId": "119a5d72-4b7b-40cf-bd96-e23072699ca5"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Laptop -> 7\n",
            "Mobile -> 10\n",
            "Tablet -> 5\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q35\n",
        "# Problem:\n",
        "# Find the product with the highest total revenue\n",
        "rdd=sc.parallelize(sales)\n",
        "total_sales=rdd.map(lambda x:(x[0],x[1]*x[2])).reduceByKey(lambda x,y:x+y)\n",
        "max_sales=total_sales.sortBy(lambda x:x[1],ascending=False).first()\n",
        "print(f\"{max_sales[0]} {max_sales[1]}\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "YM_p1NjLxtU5",
        "outputId": "a5ca9d00-616b-4334-f95c-77b4d652e5a5"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Laptop 350000\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q36\n",
        "# Problem:\n",
        "# Find products whose total revenue is greater than 100000.\n",
        "rdd=sc.parallelize(sales)\n",
        "total_sales=rdd.map(lambda x:(x[0],x[1]*x[2])).reduceByKey(lambda x,y:x+y)\n",
        "revenue=total_sales.filter(lambda x:x[1]>100000)\n",
        "print(revenue.collect())\n"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "NQrZra88zTGS",
        "outputId": "8f222cad-5000-4125-9d81-88af106d6bd2"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('Laptop', 350000), ('Mobile', 200000)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "customer_trasaction=[\n",
        " (\"C001\", 1000),\n",
        " (\"C002\", 500),\n",
        " (\"C001\", 700),\n",
        " (\"C003\", 1200),\n",
        " (\"C002\", 800),\n",
        " (\"C001\", 300),\n",
        " (\"C003\", 400),\n",
        " (\"C004\", 1500),\n",
        " (\"C002\", 200)\n",
        "]\n"
      ],
      "metadata": {
        "id": "KFR7IWEW1Kka"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Q37\n",
        "# Problem:\n",
        "# Group all transaction amounts by customer.\n",
        "rdd=sc.parallelize(customer_trasaction)\n",
        "trasactions=rdd.groupByKey().mapValues(list)\n",
        "for customer,trasacn in trasactions.collect():\n",
        "  print(f\"{customer} {trasacn}\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "IGA_DjG71TDV",
        "outputId": "e53b3e1b-5b3b-4877-f81a-e20a7f4a8177"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "C003 [1200, 400]\n",
            "C001 [1000, 700, 300]\n",
            "C002 [500, 800, 200]\n",
            "C004 [1500]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q38\n",
        "# Problem:\n",
        "# Calculate the total transaction amount for each customer.\n",
        "rdd=sc.parallelize(customer_trasaction)\n",
        "total=rdd.reduceByKey(lambda x,y:x+y)\n",
        "print(total.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "zELtpOCc2IiY",
        "outputId": "fc1d0915-eac9-4668-9c93-985cfe6b4f27"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('C003', 1600), ('C001', 2000), ('C002', 1500), ('C004', 1500)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q39\n",
        "# Problem:\n",
        "# Find the customer with the highest total transaction amount\n",
        "rdd=sc.parallelize(customer_trasaction)\n",
        "total=rdd.reduceByKey(lambda x,y:x+y)\n",
        "highest=total.sortBy(lambda x:x[1],ascending=False).first()\n",
        "print(highest)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "XRIxe7tG2iTd",
        "outputId": "081c11c1-958a-43f2-f221-f2fb3260f138"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "('C001', 2000)\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q40\n",
        "# Problem:\n",
        "# Find customers whose total transaction amount is greater than 1500.\n",
        "rdd=sc.parallelize(customer_trasaction)\n",
        "total=rdd.reduceByKey(lambda x,y:x+y)\n",
        "amount=total.filter(lambda x:x[1]>1500)\n",
        "print(amount.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "B4rjMHVu3hZr",
        "outputId": "23b805ae-e276-4d9e-a021-f3de580b97ee"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('C003', 1600), ('C001', 2000)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q41\n",
        "# Problem:\n",
        "# Read the file and display every line\n",
        "rdd=sc.textFile(\"application.csv.log\")\n",
        "for i in rdd.collect():\n",
        "  print(i)\n"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "FFpz3OT94KSV",
        "outputId": "8a1da812-d4b2-45bf-be53-ff277d247255"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "INFO Application started\n",
            "INFO Reading input file\n",
            "ERROR Failed to read file\n",
            "INFO Retrying operation\n",
            "ERROR Connection timeout\n",
            "WARN High memory usage\n",
            "INFO Operation completed\n",
            "ERROR Failed to write output\n",
            "INFO Application stopped\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q42\n",
        "# Problem:\n",
        "# Find all log entries containing ERROR.\n",
        "lines=rdd.filter(lambda x:\"ERROR\"in x).count()\n",
        "print(lines.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 183
        },
        "id": "2rC7yZD55fmZ",
        "outputId": "51f320ac-b90b-4357-ba56-b1448b017606"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "error",
          "ename": "AttributeError",
          "evalue": "'int' object has no attribute 'collect'",
          "traceback": [
            "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
            "\u001b[0;31mAttributeError\u001b[0m                            Traceback (most recent call last)",
            "\u001b[0;32m/tmp/ipykernel_1611/609621504.py\u001b[0m in \u001b[0;36m<cell line: 0>\u001b[0;34m()\u001b[0m\n\u001b[1;32m      3\u001b[0m \u001b[0;31m# Find all log entries containing ERROR.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m      4\u001b[0m \u001b[0mlines\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0mrdd\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfilter\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;32mlambda\u001b[0m \u001b[0mx\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\"ERROR\"\u001b[0m\u001b[0;32min\u001b[0m \u001b[0mx\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mcount\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m----> 5\u001b[0;31m \u001b[0mprint\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mlines\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mcollect\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
            "\u001b[0;31mAttributeError\u001b[0m: 'int' object has no attribute 'collect'"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q43\n",
        "# Problem:\n",
        "# Find the number of ERROR messages\n",
        "count=rdd.filter(lambda x:\"ERROR\" in x).count()\n",
        "print(count)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "2PBxs52C6o8i",
        "outputId": "a38d9234-6478-486a-fc72-63113c7aba6e"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "3\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q44\n",
        "# Problem:\n",
        "# Calculate the number of messages for each log level.\n",
        "count=rdd.map(lambda x: (x.split()[0],1))\n",
        "total_word=count.reduceByKey(lambda x,y:x+y)\n",
        "print(total_word.collect())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "h3nk2DhY7RYg",
        "outputId": "7944adf0-c7cf-4fc2-fe68-108a43807134"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "[('WARN', 1), ('INFO', 5), ('ERROR', 3)]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q45\n",
        "# Problem:\n",
        "# Save all numbers into an output directory named:\n",
        "# output/numbers\n",
        "# How many output part files will be generated if the RDD has 10 partitions and\n",
        "# no partition reduction is performed?\n",
        "rdd = sc.parallelize(range(1, 101), 10)\n",
        "rdd.coalesce(10).saveAsTextFile(\"output/numbers_6\")"
      ],
      "metadata": {
        "id": "rsUYMfsh9TnH"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Q46\n",
        "# Problem:\n",
        "# Save the numbers into the same output directory but make sure the data is\n",
        "# written into only one partition before saving.\n",
        "rdd.coalesce(1).saveAsTextFile(\"numbers_5\")"
      ],
      "metadata": {
        "id": "7_mnbTsK9xuO"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Q47\n",
        "# Problem:\n",
        "# Create an RDD containing numbers from 1 to 1000 with 20 partitions. Reduce\n",
        "# the number of partitions to 5 before saving\n",
        "rdd=sc.parallelize(range(1,1001),20)\n",
        "print(f\"Initial Partition {rdd.getNumPartitions()}\")\n",
        "\n",
        "rdd_5=rdd.coalesce(5)\n",
        "\n",
        "print(f\"After coalesce: {rdd_5.getNumPartitions()}\")\n",
        "\n",
        "rdd_5.saveAsTextFile(\"output/numbers_1000\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "UvSbukmJ-ZN9",
        "outputId": "926e958d-d62c-4b5f-80bd-07e5fe16715c"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Initial Partition 20\n",
            "After coalesce: 5\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q48\n",
        "# Problem:\n",
        "# Create an RDD containing numbers from 1 to 10,000 with 20 partitions and save\n",
        "# the data using only 2 output partitions.\n",
        "rdd=sc.parallelize(range(1,10000),20)\n",
        "\n",
        "print(f\"Initial partitions {rdd.getNumPartitions()}\")\n",
        "\n",
        "rdd_2=rdd.coalesce(2)\n",
        "\n",
        "print(f\"After coalesce {rdd.getNumPartitions()}\")\n",
        "\n",
        "rdd_2.saveAsTextFile(\"output/numbers_10000\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "AwjZtlvxA_vK",
        "outputId": "2cc0f34d-6491-4a91-efd4-a61a571f0821"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Initial partitions 20\n",
            "After coalesce 20\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "orders=[\n",
        " (\"O1\", \"Laptop\", 2, 50000),\n",
        " (\"O2\", \"Mobile\", 5, 20000),\n",
        " (\"O3\", \"Laptop\", 1, 50000),\n",
        " (\"O4\", \"Tablet\", 3, 15000),\n",
        " (\"O5\", \"Mobile\", 2, 20000),\n",
        " (\"O6\", \"Laptop\", 4, 50000)\n",
        "]"
      ],
      "metadata": {
        "id": "vO6jZpr4B-lC"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Q49\n",
        "# Problem:\n",
        "# Calculate the revenue of every order.\n",
        "# Revenue:\n",
        "rdd=sc.parallelize(orders)\n",
        "total_revenue=rdd.map(lambda x:(x[0],x[2]*x[3]))\n",
        "for order_id,total_price in total_revenue.collect():\n",
        "  print(f\"{order_id}-> {total_price}\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "thJzsrNzCLh0",
        "outputId": "3a6f8a5e-8769-4ccd-f765-ac9dc3b55697"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "O1-> 100000\n",
            "O2-> 100000\n",
            "O3-> 50000\n",
            "O4-> 45000\n",
            "O5-> 40000\n",
            "O6-> 200000\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "# Q50\n",
        "# Problem:\n",
        "# Perform the following complete analysis:\n",
        "# Calculate revenue for every order.\n",
        "# Keep only orders with revenue greater than 50000.\n",
        "# Calculate total revenue for each product.\n",
        "# Find the product with the highest revenue.\n",
        "# Save the final result into a single output partition.\n",
        "\n",
        "rdd=sc.parallelize(orders)\n",
        "# Calculate revenue for every order: (order_id, product_name, order_revenue)\n",
        "order_revenues=rdd.map(lambda x:(x[0],x[1],(x[2]*x[3])))\n",
        "\n",
        "print(\"Revenue for every order:\")\n",
        "for order_id, product_name, revenue in order_revenues.collect():\n",
        "  print(f\"Order {order_id} ({product_name}) -> {revenue}\")\n",
        "print(\"\\n\")\n",
        "\n",
        "\n",
        "filtered_orders_revenue=order_revenues.filter(lambda x:x[2]>50000)\n",
        "\n",
        "print(\"Orders with revenue greater than 50000:\")\n",
        "for order_id, product_name, revenue in filtered_orders_revenue.collect():\n",
        "  print(f\"Order {order_id} ({product_name}) -> {revenue}\")\n",
        "print(\"\\n\")\n",
        "\n",
        "\n",
        "product_revenue_map=rdd.map(lambda x:(x[1],x[2]*x[3]))\n",
        "total_product_revenue=product_revenue_map.reduceByKey(lambda x,y:x+y)\n",
        "\n",
        "print(\"Total revenue for each product:\")\n",
        "for product_name, total in total_product_revenue.collect():\n",
        "  print(f\"{product_name} -> {total}\")\n",
        "print(\"\\n\")\n",
        "\n",
        "\n",
        "highest_revenue_product = total_product_revenue.sortBy(lambda x: x[1], ascending=False).first()\n",
        "print(f\"Product with highest revenue: {highest_revenue_product[0]} -> {highest_revenue_product[1]}\")\n",
        "print(\"\\n\")\n",
        "\n",
        "total_product_revenue.coalesce(1).saveAsTextFile(\"output/final_revenue\")\n",
        "print(\"\\n output:1 part file\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "gSbSBwIQC0ei",
        "outputId": "d2219cdc-4e94-48ee-e342-476cc52306b7"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Revenue for every order:\n",
            "Order O1 (Laptop) -> 100000\n",
            "Order O2 (Mobile) -> 100000\n",
            "Order O3 (Laptop) -> 50000\n",
            "Order O4 (Tablet) -> 45000\n",
            "Order O5 (Mobile) -> 40000\n",
            "Order O6 (Laptop) -> 200000\n",
            "\n",
            "\n",
            "Orders with revenue greater than 50000:\n",
            "Order O1 (Laptop) -> 100000\n",
            "Order O2 (Mobile) -> 100000\n",
            "Order O6 (Laptop) -> 200000\n",
            "\n",
            "\n",
            "Total revenue for each product:\n",
            "Laptop -> 350000\n",
            "Mobile -> 140000\n",
            "Tablet -> 45000\n",
            "\n",
            "\n",
            "Product with highest revenue: Laptop -> 350000\n",
            "\n",
            "\n",
            "\n",
            " output:1 part file\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [],
      "metadata": {
        "id": "xQ_4iBiMFkpU"
      },
      "execution_count": null,
      "outputs": []
    }
  ]
}