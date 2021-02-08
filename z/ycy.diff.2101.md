---
layout: default
title: 话题统计
description: 2021年1月份
permalink: /z/d2101/
---
## 本月热榜


```python
%%time
import numpy as np

query = """
with t as (
select question_id,title,GMT_create from hot_questions 
where question_id in (select question_id from topic_index where nickname like 'ycy')
),
t0 as (select question_id,rank() over (partition by GMT_create order by rowid) as rank 
from hot_stats) 
select t.question_id,title,t1.answers,t1.heat,t1.GMT_create ,rank
from t,hot_stats t1 ,t0 
where t.question_id=t1.question_id and t.question_id = t0.question_id
and t.GMT_create between '%s' and '%s'
""" % (
    previous_startat,
    recent_startat,
)
pd.read_sql(
    query,
    con=create_engine("sqlite:////home/sopp/docker/csv/db/zhihu.db"),
    parse_dates=["GMT_create"],
).groupby(
    [
        "question_id",
        #    pd.Grouper(key="GMT_create", freq="m")
    ]
).agg(
    answers_count=("answers", np.ptp),
    heat_max=("heat", "max"),
    duration=("GMT_create", np.ptp),
    first=("GMT_create", "first"),
    title=("title", "first"),
    toprank=("rank", "min"),
).sort_values(
    #  by='duration',
    #  by='heat_max',
    by="answers_count",
    ascending=False,
).head(
    50
)
```

    CPU times: user 5.01 s, sys: 1.19 s, total: 6.21 s
    Wall time: 6.76 s





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>answers_count</th>
      <th>heat_max</th>
      <th>duration</th>
      <th>first</th>
      <th>title</th>
      <th>toprank</th>
    </tr>
    <tr>
      <th>question_id</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>437496625</th>
      <td>435</td>
      <td>594</td>
      <td>0 days 21:56:09.578301</td>
      <td>2021-01-02 13:44:24.732522</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>6</td>
    </tr>
    <tr>
      <th>432152643</th>
      <td>243</td>
      <td>210</td>
      <td>0 days 12:07:43.416673</td>
      <td>2021-01-21 10:53:46.949731</td>
      <td>请教一下杨超越哪里励志?</td>
      <td>17</td>
    </tr>
    <tr>
      <th>439440841</th>
      <td>23</td>
      <td>140</td>
      <td>0 days 08:38:07.736356</td>
      <td>2021-01-16 23:19:16.013533</td>
      <td>如何评价综艺《平行时空遇见你》第五期？</td>
      <td>39</td>
    </tr>
    <tr>
      <th>442042085</th>
      <td>19</td>
      <td>84</td>
      <td>0 days 12:35:42.142091</td>
      <td>2021-01-30 22:50:55.067448</td>
      <td>如何评价综艺《平行时空遇见你》第七期？</td>
      <td>34</td>
    </tr>
    <tr>
      <th>440563921</th>
      <td>11</td>
      <td>96</td>
      <td>0 days 08:38:01.319790</td>
      <td>2021-01-24 01:52:43.605225</td>
      <td>如何评价综艺《平行时空遇见你》第六期？</td>
      <td>20</td>
    </tr>
  </tbody>
</table>
</div>



## 当月增量数据

### 本月回答（旧问题）


```python
answers_recent[
    answers_recent.question_id.isin(questions_recent.question_id) == False
][["question_id", "answer_id", "voteups", "comments", "create_at"]].merge(
    questions[["question_id", "title"]], on="question_id"
)[
    lambda x: x.voteups > 499
].sort_values(
    by="voteups", ascending=False
)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>question_id</th>
      <th>answer_id</th>
      <th>voteups</th>
      <th>comments</th>
      <th>create_at</th>
      <th>title</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1687</th>
      <td>305761329</td>
      <td>1665601956</td>
      <td>8665</td>
      <td>671</td>
      <td>2021-01-07 13:05:04</td>
      <td>为什么没人按照杨超越的样子整？</td>
    </tr>
    <tr>
      <th>92</th>
      <td>436075658</td>
      <td>1679509322</td>
      <td>5730</td>
      <td>1117</td>
      <td>2021-01-15 16:21:11</td>
      <td>华为二公主姚安娜会不会成为第二个杨超越？</td>
    </tr>
    <tr>
      <th>1686</th>
      <td>305761329</td>
      <td>1668899868</td>
      <td>2692</td>
      <td>88</td>
      <td>2021-01-09 12:49:48</td>
      <td>为什么没人按照杨超越的样子整？</td>
    </tr>
    <tr>
      <th>72</th>
      <td>436075658</td>
      <td>1677814548</td>
      <td>1670</td>
      <td>277</td>
      <td>2021-01-14 16:51:50</td>
      <td>华为二公主姚安娜会不会成为第二个杨超越？</td>
    </tr>
    <tr>
      <th>678</th>
      <td>432152643</td>
      <td>1672255628</td>
      <td>1352</td>
      <td>185</td>
      <td>2021-01-11 14:52:56</td>
      <td>请教一下杨超越哪里励志?</td>
    </tr>
    <tr>
      <th>687</th>
      <td>432152643</td>
      <td>1689569001</td>
      <td>1122</td>
      <td>94</td>
      <td>2021-01-21 13:11:57</td>
      <td>请教一下杨超越哪里励志?</td>
    </tr>
    <tr>
      <th>37</th>
      <td>436075658</td>
      <td>1678548274</td>
      <td>725</td>
      <td>154</td>
      <td>2021-01-15 01:20:32</td>
      <td>华为二公主姚安娜会不会成为第二个杨超越？</td>
    </tr>
    <tr>
      <th>242</th>
      <td>436075658</td>
      <td>1678851831</td>
      <td>594</td>
      <td>88</td>
      <td>2021-01-15 10:14:48</td>
      <td>华为二公主姚安娜会不会成为第二个杨超越？</td>
    </tr>
  </tbody>
</table>
</div>



### 本月回答（本月问题）


```python
answers_recent[answers_recent.question_id.isin(questions_recent.question_id)][
    ["question_id", "answer_id", "voteups", "comments", "create_at"]
].merge(questions[["question_id", "title"]], on="question_id")[
    lambda x: x.voteups > 499
].sort_values(
    by="voteups", ascending=False
)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>question_id</th>
      <th>answer_id</th>
      <th>voteups</th>
      <th>comments</th>
      <th>create_at</th>
      <th>title</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1093</th>
      <td>437502621</td>
      <td>1658863540</td>
      <td>5740</td>
      <td>347</td>
      <td>2021-01-03 18:53:53</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
    </tr>
    <tr>
      <th>1392</th>
      <td>437496625</td>
      <td>1657858466</td>
      <td>5665</td>
      <td>191</td>
      <td>2021-01-03 01:07:50</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1381</th>
      <td>437496625</td>
      <td>1656911242</td>
      <td>5045</td>
      <td>123</td>
      <td>2021-01-02 12:54:57</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1378</th>
      <td>437496625</td>
      <td>1657019709</td>
      <td>4482</td>
      <td>218</td>
      <td>2021-01-02 14:20:45</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1379</th>
      <td>437496625</td>
      <td>1656764300</td>
      <td>4434</td>
      <td>279</td>
      <td>2021-01-02 11:05:12</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1377</th>
      <td>437496625</td>
      <td>1657062488</td>
      <td>3843</td>
      <td>177</td>
      <td>2021-01-02 14:56:15</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1106</th>
      <td>437502621</td>
      <td>1657260332</td>
      <td>2263</td>
      <td>114</td>
      <td>2021-01-02 17:27:56</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
    </tr>
    <tr>
      <th>1390</th>
      <td>437496625</td>
      <td>1656883373</td>
      <td>1549</td>
      <td>60</td>
      <td>2021-01-02 12:31:50</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1383</th>
      <td>437496625</td>
      <td>1656896569</td>
      <td>1362</td>
      <td>39</td>
      <td>2021-01-02 12:42:31</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1097</th>
      <td>437502621</td>
      <td>1657451636</td>
      <td>1334</td>
      <td>82</td>
      <td>2021-01-02 20:00:28</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
    </tr>
    <tr>
      <th>1095</th>
      <td>437502621</td>
      <td>1656876808</td>
      <td>1230</td>
      <td>203</td>
      <td>2021-01-02 12:26:28</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
    </tr>
    <tr>
      <th>1102</th>
      <td>437502621</td>
      <td>1656929691</td>
      <td>1170</td>
      <td>126</td>
      <td>2021-01-02 13:09:33</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
    </tr>
    <tr>
      <th>711</th>
      <td>438860677</td>
      <td>1699809940</td>
      <td>1018</td>
      <td>237</td>
      <td>2021-01-27 04:51:13</td>
      <td>杨超越和侯明昊会在一起吗？也太甜了吧？</td>
    </tr>
    <tr>
      <th>409</th>
      <td>439770906</td>
      <td>1686582576</td>
      <td>1007</td>
      <td>117</td>
      <td>2021-01-19 20:00:03</td>
      <td>为什么我觉得互联网在夸杨超越？</td>
    </tr>
    <tr>
      <th>1376</th>
      <td>437496625</td>
      <td>1657485163</td>
      <td>743</td>
      <td>63</td>
      <td>2021-01-02 20:24:52</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1385</th>
      <td>437496625</td>
      <td>1656879494</td>
      <td>742</td>
      <td>49</td>
      <td>2021-01-02 12:28:36</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1110</th>
      <td>437502621</td>
      <td>1657637672</td>
      <td>741</td>
      <td>43</td>
      <td>2021-01-02 22:13:31</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
    </tr>
    <tr>
      <th>1389</th>
      <td>437496625</td>
      <td>1656766813</td>
      <td>661</td>
      <td>100</td>
      <td>2021-01-02 11:06:55</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>1386</th>
      <td>437496625</td>
      <td>1656957690</td>
      <td>633</td>
      <td>12</td>
      <td>2021-01-02 13:31:37</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
    </tr>
    <tr>
      <th>74</th>
      <td>441959169</td>
      <td>1707780896</td>
      <td>519</td>
      <td>121</td>
      <td>2021-01-31 17:34:37</td>
      <td>侯明昊喜欢杨超越吗？</td>
    </tr>
    <tr>
      <th>249</th>
      <td>440563921</td>
      <td>1694057474</td>
      <td>514</td>
      <td>225</td>
      <td>2021-01-23 22:40:16</td>
      <td>如何评价综艺《平行时空遇见你》第六期？</td>
    </tr>
    <tr>
      <th>167</th>
      <td>440900430</td>
      <td>1695411461</td>
      <td>505</td>
      <td>104</td>
      <td>2021-01-24 19:02:32</td>
      <td>虞书欣比杨超越身高高吗？</td>
    </tr>
    <tr>
      <th>1092</th>
      <td>437502621</td>
      <td>1658000238</td>
      <td>501</td>
      <td>99</td>
      <td>2021-01-03 08:19:03</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
    </tr>
  </tbody>
</table>
</div>



### 当月问题


```python
%%time
questions_recent[
    ["question_id", "title", "create_at", "views", "answers",]
].merge(
    answers.groupby("question_id")[["voteups", "comments"]]
    .sum()
    .reset_index(),
    on="question_id",
    how="left",
).sort_values(
    by="views",
    #   by="voteups",
    ascending=False,
).head(
    10
)
```

    CPU times: user 43.4 ms, sys: 0 ns, total: 43.4 ms
    Wall time: 53 ms





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>question_id</th>
      <th>title</th>
      <th>create_at</th>
      <th>views</th>
      <th>answers</th>
      <th>voteups</th>
      <th>comments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>66</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>2021-01-02 08:37:53</td>
      <td>3056346</td>
      <td>616</td>
      <td>38829.0</td>
      <td>2554.0</td>
    </tr>
    <tr>
      <th>65</th>
      <td>437502621</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
      <td>2021-01-02 09:52:15</td>
      <td>1739000</td>
      <td>285</td>
      <td>17476.0</td>
      <td>2361.0</td>
    </tr>
    <tr>
      <th>47</th>
      <td>438860677</td>
      <td>杨超越和侯明昊会在一起吗？也太甜了吧？</td>
      <td>2021-01-11 13:37:35</td>
      <td>1422990</td>
      <td>172</td>
      <td>3346.0</td>
      <td>901.0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>440900430</td>
      <td>虞书欣比杨超越身高高吗？</td>
      <td>2021-01-24 09:44:52</td>
      <td>310917</td>
      <td>9</td>
      <td>711.0</td>
      <td>156.0</td>
    </tr>
    <tr>
      <th>40</th>
      <td>439440841</td>
      <td>如何评价综艺《平行时空遇见你》第五期？</td>
      <td>2021-01-15 08:07:05</td>
      <td>216269</td>
      <td>88</td>
      <td>2731.0</td>
      <td>614.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>441959169</td>
      <td>侯明昊喜欢杨超越吗？</td>
      <td>2021-01-30 10:06:22</td>
      <td>211820</td>
      <td>52</td>
      <td>945.0</td>
      <td>445.0</td>
    </tr>
    <tr>
      <th>20</th>
      <td>440563921</td>
      <td>如何评价综艺《平行时空遇见你》第六期？</td>
      <td>2021-01-22 05:27:51</td>
      <td>205827</td>
      <td>65</td>
      <td>1736.0</td>
      <td>822.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>441486610</td>
      <td>哪一瞬间，你突然get到了杨超越?</td>
      <td>2021-01-27 17:45:52</td>
      <td>127670</td>
      <td>82</td>
      <td>69.0</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>442042085</td>
      <td>如何评价综艺《平行时空遇见你》第七期？</td>
      <td>2021-01-30 20:01:02</td>
      <td>126509</td>
      <td>71</td>
      <td>1688.0</td>
      <td>451.0</td>
    </tr>
    <tr>
      <th>44</th>
      <td>438915911</td>
      <td>你觉得明月夜CP如何？</td>
      <td>2021-01-11 19:56:50</td>
      <td>116279</td>
      <td>67</td>
      <td>662.0</td>
      <td>207.0</td>
    </tr>
  </tbody>
</table>
</div>



## 存量数据对比

### 旧回答评论、获赞数


```python
diffa = (
    answers_previous[
        ["question_id", "answer_id", "voteups", "comments", "create_at"]
    ]
    .merge(answers[["answer_id", "voteups", "comments"]], on="answer_id")
    .merge(questions_previous[["question_id", "title"]], on="question_id")
)
diffa["voteups_inc"] = diffa.voteups_y - diffa.voteups_x
diffa["comments_inc"] = diffa.comments_y - diffa.comments_x
diffa[(diffa["voteups_inc"] > 499) | (diffa["comments_inc"] > 49)][
    [
        "question_id",
        "answer_id",
        "voteups_inc",
        "comments_inc",
        "title",
        "create_at",
    ]
].sort_values(by="voteups_inc", ascending=False)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>question_id</th>
      <th>answer_id</th>
      <th>voteups_inc</th>
      <th>comments_inc</th>
      <th>title</th>
      <th>create_at</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4686</th>
      <td>432115857</td>
      <td>1624314765</td>
      <td>13732</td>
      <td>908</td>
      <td>杨超越和丁真的性质有什么不同？</td>
      <td>2020-12-13 06:10:31</td>
    </tr>
    <tr>
      <th>131164</th>
      <td>302272818</td>
      <td>1515117348</td>
      <td>1935</td>
      <td>183</td>
      <td>长得像杨超越是一种什么体验？</td>
      <td>2020-10-09 19:30:36</td>
    </tr>
    <tr>
      <th>21481</th>
      <td>408938147</td>
      <td>1381234546</td>
      <td>766</td>
      <td>76</td>
      <td>杨超越有哪些出圈神图?</td>
      <td>2020-08-03 16:41:54</td>
    </tr>
    <tr>
      <th>4682</th>
      <td>432115857</td>
      <td>1604031915</td>
      <td>696</td>
      <td>22</td>
      <td>杨超越和丁真的性质有什么不同？</td>
      <td>2020-11-30 22:47:11</td>
    </tr>
    <tr>
      <th>510</th>
      <td>434035876</td>
      <td>1619255609</td>
      <td>664</td>
      <td>15</td>
      <td>为什么这么多人骂丁真，杨超越不是一样的吗，却这么多人喜欢？</td>
      <td>2020-12-09 21:37:46</td>
    </tr>
    <tr>
      <th>14131</th>
      <td>419628591</td>
      <td>1475926336</td>
      <td>598</td>
      <td>22</td>
      <td>如何看待杨超越说种 100 斤玉米只能买 3 斤猪肉?</td>
      <td>2020-09-16 13:26:11</td>
    </tr>
    <tr>
      <th>3229</th>
      <td>432537893</td>
      <td>1611496968</td>
      <td>549</td>
      <td>35</td>
      <td>如何看待杨超越米饭掉了捡起来吃？</td>
      <td>2020-12-05 08:59:58</td>
    </tr>
    <tr>
      <th>131177</th>
      <td>302272818</td>
      <td>944043049</td>
      <td>180</td>
      <td>109</td>
      <td>长得像杨超越是一种什么体验？</td>
      <td>2019-12-20 23:44:07</td>
    </tr>
    <tr>
      <th>4684</th>
      <td>432115857</td>
      <td>1639360040</td>
      <td>73</td>
      <td>110</td>
      <td>杨超越和丁真的性质有什么不同？</td>
      <td>2020-12-22 11:00:54</td>
    </tr>
    <tr>
      <th>118837</th>
      <td>307531216</td>
      <td>1627262275</td>
      <td>65</td>
      <td>87</td>
      <td>你为什么会脱粉杨超越？</td>
      <td>2020-12-14 23:38:01</td>
    </tr>
    <tr>
      <th>12396</th>
      <td>422363398</td>
      <td>1491335969</td>
      <td>10</td>
      <td>304</td>
      <td>为什么杨超越能拿下车代？</td>
      <td>2020-09-24 20:08:05</td>
    </tr>
    <tr>
      <th>11642</th>
      <td>423554950</td>
      <td>1522577379</td>
      <td>8</td>
      <td>53</td>
      <td>到底是谁在捧红杨超越？</td>
      <td>2020-10-14 00:48:02</td>
    </tr>
    <tr>
      <th>170132</th>
      <td>278726560</td>
      <td>409677722</td>
      <td>0</td>
      <td>84</td>
      <td>如何看待《创造101》第六期杨超越排名第二？</td>
      <td>2018-06-05 08:51:28</td>
    </tr>
    <tr>
      <th>54711</th>
      <td>367912247</td>
      <td>992452552</td>
      <td>0</td>
      <td>78</td>
      <td>为什么诸葛大力（成果、狗哥）的火，会引起部分女生的反感？</td>
      <td>2020-01-31 21:08:22</td>
    </tr>
    <tr>
      <th>176179</th>
      <td>274328777</td>
      <td>378925849</td>
      <td>-1</td>
      <td>163</td>
      <td>如何评价《创造101》的杨超越？</td>
      <td>2018-04-29 11:51:12</td>
    </tr>
    <tr>
      <th>149763</th>
      <td>289660032</td>
      <td>465739101</td>
      <td>-3</td>
      <td>76</td>
      <td>如何看待目前杨超越和王思聪组成的洋葱CP？</td>
      <td>2018-08-10 20:06:29</td>
    </tr>
    <tr>
      <th>59347</th>
      <td>364082405</td>
      <td>959543420</td>
      <td>-5</td>
      <td>757</td>
      <td>如何评价杨超越在湖南卫视2020跨年演唱会上的表现？</td>
      <td>2020-01-02 11:38:55</td>
    </tr>
    <tr>
      <th>54713</th>
      <td>367912247</td>
      <td>995731998</td>
      <td>-15</td>
      <td>1867</td>
      <td>为什么诸葛大力（成果、狗哥）的火，会引起部分女生的反感？</td>
      <td>2020-02-02 23:28:09</td>
    </tr>
  </tbody>
</table>
</div>



### 旧问题浏览数,回答，获赞 todo

得跑过上面之后才有获赞数据,没包括旧问题中新回答的赞数


```python
diffq = (
    questions_previous[
        ["question_id", "title", "answers", "views", "create_at"]
    ]
    .merge(
        questions[["question_id", "answers", "views"]],
        on="question_id",
        how="left",
    )
    .merge(
        diffa.groupby("question_id")[["voteups_inc", "comments_inc"]]
        .sum()
        .reset_index(),
        on="question_id",
        how="left",
    )
    .merge(
        answers_previous.groupby("question_id")[["voteups", "comments"]]
        .sum()
        .reset_index(),
        on="question_id",
        how="left",
    )
    .merge(
        answers.groupby("question_id")[["voteups", "comments"]]
        .sum()
        .reset_index(),
        on="question_id",
        how="left",
    )
)
diffq["answers_inc"] = diffq.answers_y - diffq.answers_x
diffq["views_inc"] = diffq.views_y - diffq.views_x
diffq["voteups_inc2"] = diffq.voteups_y - diffq.voteups_x
diffq["comments_inc2"] = diffq.comments_y - diffq.comments_x
diffq[
    (diffq["answers_inc"] > 19)
    | (diffq["views_inc"] > 199999)
    | (diffq["voteups_inc"] > 999)
    | (diffq["comments_inc"] > 99)
][
    [
        "question_id",
        "title",
        "create_at",
        "answers_inc",
        "views_inc",
        "voteups_inc2",
        "comments_inc2",
        #       "voteups",
        #      "comments",
    ]
].sort_values(
    by="views_inc", ascending=False
)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>question_id</th>
      <th>title</th>
      <th>create_at</th>
      <th>answers_inc</th>
      <th>views_inc</th>
      <th>voteups_inc2</th>
      <th>comments_inc2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>12</th>
      <td>436075658</td>
      <td>华为二公主姚安娜会不会成为第二个杨超越？</td>
      <td>2020-12-23 02:01:50</td>
      <td>423.0</td>
      <td>1859638.0</td>
      <td>12139.0</td>
      <td>2413.0</td>
    </tr>
    <tr>
      <th>3103</th>
      <td>305761329</td>
      <td>为什么没人按照杨超越的样子整？</td>
      <td>2018-12-14 23:37:08</td>
      <td>65.0</td>
      <td>1819162.0</td>
      <td>12144.0</td>
      <td>1081.0</td>
    </tr>
    <tr>
      <th>82</th>
      <td>432152643</td>
      <td>请教一下杨超越哪里励志?</td>
      <td>2020-11-26 21:40:00</td>
      <td>318.0</td>
      <td>722465.0</td>
      <td>5987.0</td>
      <td>1127.0</td>
    </tr>
    <tr>
      <th>86</th>
      <td>432115857</td>
      <td>杨超越和丁真的性质有什么不同？</td>
      <td>2020-11-26 17:18:03</td>
      <td>80.0</td>
      <td>644937.0</td>
      <td>15704.0</td>
      <td>1332.0</td>
    </tr>
    <tr>
      <th>3304</th>
      <td>302272818</td>
      <td>长得像杨超越是一种什么体验？</td>
      <td>2018-11-13 15:22:36</td>
      <td>13.0</td>
      <td>570736.0</td>
      <td>2652.0</td>
      <td>348.0</td>
    </tr>
    <tr>
      <th>1233</th>
      <td>379710349</td>
      <td>虞书欣与杨超越有哪些差别？</td>
      <td>2020-03-15 20:18:32</td>
      <td>8.0</td>
      <td>282341.0</td>
      <td>1242.0</td>
      <td>70.0</td>
    </tr>
    <tr>
      <th>2989</th>
      <td>307531216</td>
      <td>你为什么会脱粉杨超越？</td>
      <td>2018-12-31 10:35:40</td>
      <td>71.0</td>
      <td>145841.0</td>
      <td>653.0</td>
      <td>414.0</td>
    </tr>
    <tr>
      <th>3334</th>
      <td>301720700</td>
      <td>杨超越如果在韩国出道能在韩团做门面吗？</td>
      <td>2018-11-08 13:20:27</td>
      <td>23.0</td>
      <td>140897.0</td>
      <td>790.0</td>
      <td>11.0</td>
    </tr>
    <tr>
      <th>73</th>
      <td>432537893</td>
      <td>如何看待杨超越米饭掉了捡起来吃？</td>
      <td>2020-11-29 15:49:25</td>
      <td>52.0</td>
      <td>125456.0</td>
      <td>1027.0</td>
      <td>56.0</td>
    </tr>
    <tr>
      <th>2996</th>
      <td>307407547</td>
      <td>“王菊现象”比“杨超越现象”为什么后劲不足？明明开局更好！王菊为什么反转杨超越后又被杨超越反转？</td>
      <td>2018-12-29 23:20:08</td>
      <td>39.0</td>
      <td>100679.0</td>
      <td>1161.0</td>
      <td>118.0</td>
    </tr>
    <tr>
      <th>3004</th>
      <td>307265369</td>
      <td>如何评价刘亦菲和杨超越的颜值?</td>
      <td>2018-12-28 16:22:23</td>
      <td>43.0</td>
      <td>100153.0</td>
      <td>500.0</td>
      <td>274.0</td>
    </tr>
    <tr>
      <th>317</th>
      <td>419628591</td>
      <td>如何看待杨超越说种 100 斤玉米只能买 3 斤猪肉?</td>
      <td>2020-09-06 00:33:28</td>
      <td>-22.0</td>
      <td>97689.0</td>
      <td>893.0</td>
      <td>36.0</td>
    </tr>
    <tr>
      <th>4367</th>
      <td>278822083</td>
      <td>为什么你不喜欢杨超越？</td>
      <td>2018-05-27 16:07:57</td>
      <td>30.0</td>
      <td>39993.0</td>
      <td>268.0</td>
      <td>210.0</td>
    </tr>
    <tr>
      <th>85</th>
      <td>432127237</td>
      <td>为什么杨超越的死忠那么多?</td>
      <td>2020-11-26 18:38:57</td>
      <td>26.0</td>
      <td>26383.0</td>
      <td>499.0</td>
      <td>98.0</td>
    </tr>
    <tr>
      <th>94</th>
      <td>432000892</td>
      <td>怎么就有那么多人会喜欢杨超越？</td>
      <td>2020-11-25 22:27:45</td>
      <td>24.0</td>
      <td>20780.0</td>
      <td>514.0</td>
      <td>-7.0</td>
    </tr>
    <tr>
      <th>4408</th>
      <td>274328777</td>
      <td>如何评价《创造101》的杨超越？</td>
      <td>2018-04-23 07:34:26</td>
      <td>-4.0</td>
      <td>11554.0</td>
      <td>66.0</td>
      <td>250.0</td>
    </tr>
    <tr>
      <th>2059</th>
      <td>334284846</td>
      <td>杨超越粉丝如何看待杨超越吻戏?</td>
      <td>2019-07-10 19:10:18</td>
      <td>29.0</td>
      <td>11024.0</td>
      <td>52.0</td>
      <td>16.0</td>
    </tr>
    <tr>
      <th>1361</th>
      <td>367912247</td>
      <td>为什么诸葛大力（成果、狗哥）的火，会引起部分女生的反感？</td>
      <td>2020-01-26 01:43:47</td>
      <td>-16.0</td>
      <td>10481.0</td>
      <td>-198.0</td>
      <td>1844.0</td>
    </tr>
    <tr>
      <th>259</th>
      <td>422363398</td>
      <td>为什么杨超越能拿下车代？</td>
      <td>2020-09-22 14:33:41</td>
      <td>-2.0</td>
      <td>5209.0</td>
      <td>19.0</td>
      <td>307.0</td>
    </tr>
    <tr>
      <th>2852</th>
      <td>310444557</td>
      <td>喜欢杨超越的粉丝们，你们多少岁了？</td>
      <td>2019-01-27 17:47:46</td>
      <td>24.0</td>
      <td>4175.0</td>
      <td>141.0</td>
      <td>26.0</td>
    </tr>
    <tr>
      <th>1427</th>
      <td>364082405</td>
      <td>如何评价杨超越在湖南卫视2020跨年演唱会上的表现？</td>
      <td>2020-01-01 11:10:13</td>
      <td>0.0</td>
      <td>249.0</td>
      <td>-8.0</td>
      <td>763.0</td>
    </tr>
  </tbody>
</table>
</div>



## 简单查询对比todo

口径是抓取时间，不是自然月，和上面有差异


```python
summary_dict = {
    "questions": "select count(question_id) as questions from questions",
    "views": "select sum(views) as views from questions",
    "questions_noanswer": "select count(question_id) as questions_noanswer from questions where question_id not in (select distinct question_id from answers)",
    "questions_novote": "select count(*) as questions_novote from (select count(*) from answers group by question_id  having sum(voteups) <10)",
    "questions_1mview": "select count(question_id) as questions_1mview from questions where views >1000000",
    "questions_maxview": "select max(views) as questions_maxview from questions",
    "questions_maxsumvoteup": "select max(t) as questions_maxsumvoteup from (select sum(voteups) as t from answers group by question_id)",
    "answers": "select count(answer_id) as answers from answers",
    "answers_anonymous": "select count(answer_id) as answers_anonymous from answers where create_by ='0'",
    "voteups_anonymous": "select sum(voteups) as voteups_anonymous from answers where create_by ='0'",
    "answers_novoteup": "select count(answer_id) as answers_novoteup from answers where voteups <5",
    "answers_1kvoteup": "select count(answer_id) as answers_1kvoteup from answers where voteups >1000",
    #   "first_1k": "select strftime('%Y.%m.%d',min(create_at)) as first_1k from (select create_at from answers where voteups >1000)",
    #   "last_1k": "select strftime('%Y.%m.%d',max(create_at)) as last_1k from (select create_at from answers where voteups >1000)",
    "voteups": "select sum(voteups) as voteups from answers",
    "answers_maxvoteup": "select max(voteups) as answers_maxvoteup from answers",
    "answers_sumcomment": "select sum(comments) as answers_sumcomment from answers",
    "locked_byhot": 'select count(question_id) as locked_byhot from modify where modify_by is "zhihuadmin" and reason like "%热榜%"',
    #  "modify_50": "select count(*) as modify_50 from (select count(*) from modify group by question_id having count(modify_id)>50)",
    "aq": "select count(*) as user_aq from (select create_by from answers where voteups>4 group by create_by having count(answer_id)>4)",
}


def to_query(d: dict) -> str:
    p = ""
    for i in d.values():
        p = p + "(" + i + ") nature join "
    return "select * from( " + p[:-12] + ")"
```


```python
%%time
query = to_query(summary_dict)


pd.read_sql_query(query, con=engine).append(
    pd.read_sql_query(query, con=engine2)
).append(
    pd.read_sql_query(query, con=engine)
    - pd.read_sql_query(query, con=engine2)
).T
```

    CPU times: user 3.74 s, sys: 2.27 s, total: 6.01 s
    Wall time: 7.09 s





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>0</th>
      <th>0</th>
      <th>0</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>questions</th>
      <td>4492</td>
      <td>4428</td>
      <td>64</td>
    </tr>
    <tr>
      <th>views</th>
      <td>913351052</td>
      <td>899011297</td>
      <td>14339755</td>
    </tr>
    <tr>
      <th>questions_noanswer</th>
      <td>641</td>
      <td>626</td>
      <td>15</td>
    </tr>
    <tr>
      <th>questions_novote</th>
      <td>1426</td>
      <td>1422</td>
      <td>4</td>
    </tr>
    <tr>
      <th>questions_1mview</th>
      <td>208</td>
      <td>203</td>
      <td>5</td>
    </tr>
    <tr>
      <th>questions_maxview</th>
      <td>50023664</td>
      <td>49989971</td>
      <td>33693</td>
    </tr>
    <tr>
      <th>questions_maxsumvoteup</th>
      <td>158812</td>
      <td>158849</td>
      <td>-37</td>
    </tr>
    <tr>
      <th>answers</th>
      <td>184203</td>
      <td>181655</td>
      <td>2548</td>
    </tr>
    <tr>
      <th>answers_anonymous</th>
      <td>41577</td>
      <td>41005</td>
      <td>572</td>
    </tr>
    <tr>
      <th>voteups_anonymous</th>
      <td>650476</td>
      <td>643646</td>
      <td>6830</td>
    </tr>
    <tr>
      <th>answers_novoteup</th>
      <td>115625</td>
      <td>113938</td>
      <td>1687</td>
    </tr>
    <tr>
      <th>answers_1kvoteup</th>
      <td>863</td>
      <td>851</td>
      <td>12</td>
    </tr>
    <tr>
      <th>voteups</th>
      <td>6584788</td>
      <td>6490741</td>
      <td>94047</td>
    </tr>
    <tr>
      <th>answers_maxvoteup</th>
      <td>53142</td>
      <td>53159</td>
      <td>-17</td>
    </tr>
    <tr>
      <th>answers_sumcomment</th>
      <td>1043862</td>
      <td>1030009</td>
      <td>13853</td>
    </tr>
    <tr>
      <th>locked_byhot</th>
      <td>346</td>
      <td>341</td>
      <td>5</td>
    </tr>
    <tr>
      <th>user_aq</th>
      <td>1956</td>
      <td>1929</td>
      <td>27</td>
    </tr>
  </tbody>
</table>
</div>



## 俱乐部新成员 

### 1m浏览问题


```python
questions[questions.views > 1000000][
    lambda x: x.question_id.isin(
        questions_previous[questions_previous.views > 1000000].question_id
    )
    == False
][["question_id", "title", "create_at", "views", "answers"]]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>question_id</th>
      <th>title</th>
      <th>create_at</th>
      <th>views</th>
      <th>answers</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>47</th>
      <td>438860677</td>
      <td>杨超越和侯明昊会在一起吗？也太甜了吧？</td>
      <td>2021-01-11 13:37:35</td>
      <td>1422990</td>
      <td>172</td>
    </tr>
    <tr>
      <th>65</th>
      <td>437502621</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
      <td>2021-01-02 09:52:15</td>
      <td>1739000</td>
      <td>285</td>
    </tr>
    <tr>
      <th>66</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>2021-01-02 08:37:53</td>
      <td>3056346</td>
      <td>616</td>
    </tr>
    <tr>
      <th>84</th>
      <td>436075658</td>
      <td>华为二公主姚安娜会不会成为第二个杨超越？</td>
      <td>2020-12-23 02:01:50</td>
      <td>1864651</td>
      <td>437</td>
    </tr>
    <tr>
      <th>2843</th>
      <td>311624330</td>
      <td>如何评价《横冲直撞20岁》的第五期？</td>
      <td>2019-02-09 13:06:12</td>
      <td>1000121</td>
      <td>211</td>
    </tr>
    <tr>
      <th>3172</th>
      <td>305761329</td>
      <td>为什么没人按照杨超越的样子整？</td>
      <td>2018-12-14 23:37:08</td>
      <td>1865693</td>
      <td>122</td>
    </tr>
  </tbody>
</table>
</div>



### 1k点赞回答


```python
answers[answers.voteups > 1000][
    lambda x: x.answer_id.isin(
        answers_previous[answers_previous.voteups > 1000].answer_id
    )
    == False
].merge(questions[["question_id", "title"]], on="question_id")[
    ["question_id", "title", "answer_id", "voteups", "comments", "create_at"]
].sort_values(
    by="create_at"
)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>question_id</th>
      <th>title</th>
      <th>answer_id</th>
      <th>voteups</th>
      <th>comments</th>
      <th>create_at</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>21</th>
      <td>301720700</td>
      <td>杨超越如果在韩国出道能在韩团做门面吗？</td>
      <td>1010216205</td>
      <td>1011</td>
      <td>157</td>
      <td>2020-02-11 15:26:12</td>
    </tr>
    <tr>
      <th>20</th>
      <td>301720700</td>
      <td>杨超越如果在韩国出道能在韩团做门面吗？</td>
      <td>1554676226</td>
      <td>1051</td>
      <td>127</td>
      <td>2020-11-02 11:02:48</td>
    </tr>
    <tr>
      <th>9</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>1656764300</td>
      <td>4434</td>
      <td>279</td>
      <td>2021-01-02 11:05:12</td>
    </tr>
    <tr>
      <th>3</th>
      <td>437502621</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
      <td>1656876808</td>
      <td>1230</td>
      <td>203</td>
      <td>2021-01-02 12:26:28</td>
    </tr>
    <tr>
      <th>12</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>1656883373</td>
      <td>1549</td>
      <td>60</td>
      <td>2021-01-02 12:31:50</td>
    </tr>
    <tr>
      <th>11</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>1656896569</td>
      <td>1362</td>
      <td>39</td>
      <td>2021-01-02 12:42:31</td>
    </tr>
    <tr>
      <th>10</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>1656911242</td>
      <td>5045</td>
      <td>123</td>
      <td>2021-01-02 12:54:57</td>
    </tr>
    <tr>
      <th>5</th>
      <td>437502621</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
      <td>1656929691</td>
      <td>1170</td>
      <td>126</td>
      <td>2021-01-02 13:09:33</td>
    </tr>
    <tr>
      <th>8</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>1657019709</td>
      <td>4482</td>
      <td>218</td>
      <td>2021-01-02 14:20:45</td>
    </tr>
    <tr>
      <th>7</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>1657062488</td>
      <td>3843</td>
      <td>177</td>
      <td>2021-01-02 14:56:15</td>
    </tr>
    <tr>
      <th>6</th>
      <td>437502621</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
      <td>1657260332</td>
      <td>2263</td>
      <td>114</td>
      <td>2021-01-02 17:27:56</td>
    </tr>
    <tr>
      <th>4</th>
      <td>437502621</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
      <td>1657451636</td>
      <td>1334</td>
      <td>82</td>
      <td>2021-01-02 20:00:28</td>
    </tr>
    <tr>
      <th>13</th>
      <td>437496625</td>
      <td>如何看待薛之谦在某公益节目让卖菜阿姨卖菜时加点土增重，是否在劝导欺骗消费者？</td>
      <td>1657858466</td>
      <td>5665</td>
      <td>191</td>
      <td>2021-01-03 01:07:50</td>
    </tr>
    <tr>
      <th>2</th>
      <td>437502621</td>
      <td>怎么看待杨超越说薛之谦「往菜里掺土」的玩笑一点都不好笑？</td>
      <td>1658863540</td>
      <td>5740</td>
      <td>347</td>
      <td>2021-01-03 18:53:53</td>
    </tr>
    <tr>
      <th>19</th>
      <td>305761329</td>
      <td>为什么没人按照杨超越的样子整？</td>
      <td>1665601956</td>
      <td>8665</td>
      <td>671</td>
      <td>2021-01-07 13:05:04</td>
    </tr>
    <tr>
      <th>18</th>
      <td>305761329</td>
      <td>为什么没人按照杨超越的样子整？</td>
      <td>1668899868</td>
      <td>2692</td>
      <td>88</td>
      <td>2021-01-09 12:49:48</td>
    </tr>
    <tr>
      <th>16</th>
      <td>432152643</td>
      <td>请教一下杨超越哪里励志?</td>
      <td>1672255628</td>
      <td>1352</td>
      <td>185</td>
      <td>2021-01-11 14:52:56</td>
    </tr>
    <tr>
      <th>14</th>
      <td>436075658</td>
      <td>华为二公主姚安娜会不会成为第二个杨超越？</td>
      <td>1677814548</td>
      <td>1670</td>
      <td>277</td>
      <td>2021-01-14 16:51:50</td>
    </tr>
    <tr>
      <th>15</th>
      <td>436075658</td>
      <td>华为二公主姚安娜会不会成为第二个杨超越？</td>
      <td>1679509322</td>
      <td>5730</td>
      <td>1117</td>
      <td>2021-01-15 16:21:11</td>
    </tr>
    <tr>
      <th>0</th>
      <td>439770906</td>
      <td>为什么我觉得互联网在夸杨超越？</td>
      <td>1686582576</td>
      <td>1007</td>
      <td>117</td>
      <td>2021-01-19 20:00:03</td>
    </tr>
    <tr>
      <th>17</th>
      <td>432152643</td>
      <td>请教一下杨超越哪里励志?</td>
      <td>1689569001</td>
      <td>1122</td>
      <td>94</td>
      <td>2021-01-21 13:11:57</td>
    </tr>
    <tr>
      <th>1</th>
      <td>438860677</td>
      <td>杨超越和侯明昊会在一起吗？也太甜了吧？</td>
      <td>1699809940</td>
      <td>1018</td>
      <td>237</td>
      <td>2021-01-27 04:51:13</td>
    </tr>
  </tbody>
</table>
</div>



### 活跃参与者

口径不是自然月


```python
query_aq = """
select create_by,min(create_at) as first,max(create_at) as last,count(answer_id) as c
from answers where create_by in 
    (select create_by from answers where voteups >4 group by create_by having count(answer_id) >4)
group by create_by
"""


def get_aq(e=engine, query=query_aq):
    aq = pd.read_sql_query(query, con=e, parse_dates=["first", "last"])
    aq["ptp"] = aq["last"] - aq["first"]
    return aq


get_aq()[
    lambda x: x.create_by.isin(get_aq(e=engine2).create_by) == False
].sort_values(by="last")
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>create_by</th>
      <th>first</th>
      <th>last</th>
      <th>c</th>
      <th>ptp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1880</th>
      <td>f5b12f004da417eea6da4d3b945949e5</td>
      <td>2020-05-28 14:31:04</td>
      <td>2020-11-02 10:23:34</td>
      <td>7</td>
      <td>157 days 19:52:30</td>
    </tr>
    <tr>
      <th>866</th>
      <td>7144b7423105995e4782c19797d0f187</td>
      <td>2019-09-05 15:51:49</td>
      <td>2020-12-19 18:14:47</td>
      <td>11</td>
      <td>471 days 02:22:58</td>
    </tr>
    <tr>
      <th>1365</th>
      <td>afc0466a89efb73a3b39f7a0cbf970dc</td>
      <td>2020-10-14 23:28:54</td>
      <td>2020-12-27 10:54:22</td>
      <td>10</td>
      <td>73 days 11:25:28</td>
    </tr>
    <tr>
      <th>282</th>
      <td>24b0d75cd58d755b0f2f39516b9d847c</td>
      <td>2020-07-17 15:10:24</td>
      <td>2021-01-01 10:07:00</td>
      <td>16</td>
      <td>167 days 18:56:36</td>
    </tr>
    <tr>
      <th>338</th>
      <td>2c707a4079aedb81181c0b04815d48db</td>
      <td>2018-05-02 16:11:36</td>
      <td>2021-01-07 23:36:35</td>
      <td>6</td>
      <td>981 days 07:24:59</td>
    </tr>
    <tr>
      <th>1398</th>
      <td>b4737286ec425cdac1f9fb5eaa76e6c1</td>
      <td>2020-07-18 18:08:00</td>
      <td>2021-01-09 16:18:51</td>
      <td>13</td>
      <td>174 days 22:10:51</td>
    </tr>
    <tr>
      <th>1043</th>
      <td>87c3fc49d4ab8ada8df0d5841f64da6e</td>
      <td>2020-01-08 17:49:38</td>
      <td>2021-01-09 23:57:35</td>
      <td>9</td>
      <td>367 days 06:07:57</td>
    </tr>
    <tr>
      <th>77</th>
      <td>0ac4eff81f60bf0346da5742996bec8f</td>
      <td>2019-09-10 13:09:39</td>
      <td>2021-01-11 21:42:56</td>
      <td>8</td>
      <td>489 days 08:33:17</td>
    </tr>
    <tr>
      <th>1273</th>
      <td>a566ce87fa0a276b6823d370b494e8f2</td>
      <td>2020-05-06 21:25:14</td>
      <td>2021-01-13 08:10:22</td>
      <td>9</td>
      <td>251 days 10:45:08</td>
    </tr>
    <tr>
      <th>73</th>
      <td>0a08485f3b60e029cff156881ff18e3f</td>
      <td>2020-02-18 19:27:49</td>
      <td>2021-01-15 23:40:05</td>
      <td>17</td>
      <td>332 days 04:12:16</td>
    </tr>
    <tr>
      <th>1422</th>
      <td>b71513a75bed208befafec14c129da56</td>
      <td>2020-05-20 05:49:43</td>
      <td>2021-01-16 21:06:02</td>
      <td>10</td>
      <td>241 days 15:16:19</td>
    </tr>
    <tr>
      <th>495</th>
      <td>3f910dc509fde4b709cdf0145751c3b7</td>
      <td>2019-05-30 19:35:03</td>
      <td>2021-01-20 20:00:04</td>
      <td>13</td>
      <td>601 days 00:25:01</td>
    </tr>
    <tr>
      <th>891</th>
      <td>7491a86e7fef58c10e6723b24136ea4b</td>
      <td>2020-07-25 00:17:28</td>
      <td>2021-01-21 12:35:34</td>
      <td>6</td>
      <td>180 days 12:18:06</td>
    </tr>
    <tr>
      <th>1708</th>
      <td>dc27f715c9f457b70b893e6827f2a7c4</td>
      <td>2020-03-24 01:21:27</td>
      <td>2021-01-24 16:28:33</td>
      <td>29</td>
      <td>306 days 15:07:06</td>
    </tr>
    <tr>
      <th>141</th>
      <td>147b5bfddd48b45e73898c50c5086579</td>
      <td>2020-02-08 02:03:32</td>
      <td>2021-01-25 04:56:45</td>
      <td>7</td>
      <td>352 days 02:53:13</td>
    </tr>
    <tr>
      <th>1612</th>
      <td>cff1fd2c780c7a86864c8279dce7cf40</td>
      <td>2019-07-06 15:55:16</td>
      <td>2021-01-25 13:07:04</td>
      <td>10</td>
      <td>568 days 21:11:48</td>
    </tr>
    <tr>
      <th>643</th>
      <td>52b9d351a33669fa923814b0e6176e44</td>
      <td>2018-07-23 13:09:11</td>
      <td>2021-01-25 17:16:02</td>
      <td>6</td>
      <td>917 days 04:06:51</td>
    </tr>
    <tr>
      <th>463</th>
      <td>3b72442ab38f69d777ea8aa2814ed835</td>
      <td>2019-10-21 11:08:07</td>
      <td>2021-01-26 17:36:27</td>
      <td>18</td>
      <td>463 days 06:28:20</td>
    </tr>
    <tr>
      <th>67</th>
      <td>08fa8fc42bedabe093715910c33caa61</td>
      <td>2019-11-13 14:41:23</td>
      <td>2021-01-30 12:32:20</td>
      <td>16</td>
      <td>443 days 21:50:57</td>
    </tr>
    <tr>
      <th>1809</th>
      <td>ebdf4198701695aa1b3e30079a9bc3ab</td>
      <td>2019-10-02 01:25:23</td>
      <td>2021-02-01 11:28:37</td>
      <td>16</td>
      <td>488 days 10:03:14</td>
    </tr>
    <tr>
      <th>1514</th>
      <td>c32f147329c8cfaa3a2f6ab26f7f6e6e</td>
      <td>2020-01-05 01:35:04</td>
      <td>2021-02-02 01:02:17</td>
      <td>8</td>
      <td>393 days 23:27:13</td>
    </tr>
    <tr>
      <th>730</th>
      <td>5e71589ac14b06b05f013e367c06a1ff</td>
      <td>2020-08-17 19:59:58</td>
      <td>2021-02-02 09:07:26</td>
      <td>5</td>
      <td>168 days 13:07:28</td>
    </tr>
    <tr>
      <th>1646</th>
      <td>d3f227d1e63976fa8f7de3baa3581f8b</td>
      <td>2020-09-17 20:07:08</td>
      <td>2021-02-02 18:11:25</td>
      <td>12</td>
      <td>137 days 22:04:17</td>
    </tr>
    <tr>
      <th>249</th>
      <td>20ea37dbb22413744b9e1ef258f8c3cb</td>
      <td>2020-11-25 12:34:58</td>
      <td>2021-02-02 20:38:00</td>
      <td>8</td>
      <td>69 days 08:03:02</td>
    </tr>
    <tr>
      <th>670</th>
      <td>5616777f85cb190a682f909a023cb85d</td>
      <td>2020-12-22 10:32:45</td>
      <td>2021-02-03 09:07:31</td>
      <td>20</td>
      <td>42 days 22:34:46</td>
    </tr>
    <tr>
      <th>1184</th>
      <td>9a8d5b821dc0c7cd214ddc194cdc3cc7</td>
      <td>2020-12-09 12:38:18</td>
      <td>2021-02-03 12:43:51</td>
      <td>16</td>
      <td>56 days 00:05:33</td>
    </tr>
    <tr>
      <th>1241</th>
      <td>a1b50066da9eb08a4ecb0706685014ef</td>
      <td>2020-05-01 00:00:34</td>
      <td>2021-02-03 18:44:41</td>
      <td>16</td>
      <td>278 days 18:44:07</td>
    </tr>
    <tr>
      <th>1767</th>
      <td>e506ce751a4ebf45978b5667cfa3074d</td>
      <td>2020-08-21 11:37:34</td>
      <td>2021-02-03 20:57:24</td>
      <td>11</td>
      <td>166 days 09:19:50</td>
    </tr>
    <tr>
      <th>852</th>
      <td>7009564a7909ccb89636fd0776c82edc</td>
      <td>2020-10-14 10:12:44</td>
      <td>2021-02-04 10:12:01</td>
      <td>12</td>
      <td>112 days 23:59:17</td>
    </tr>
    <tr>
      <th>1183</th>
      <td>9a5d594aa6b58647c96652f94605e093</td>
      <td>2020-09-15 15:22:59</td>
      <td>2021-02-04 11:27:36</td>
      <td>30</td>
      <td>141 days 20:04:37</td>
    </tr>
    <tr>
      <th>918</th>
      <td>77fd3107dfca70e4a4b90549415cdf97</td>
      <td>2019-03-16 15:23:44</td>
      <td>2021-02-05 05:42:38</td>
      <td>17</td>
      <td>691 days 14:18:54</td>
    </tr>
    <tr>
      <th>1000</th>
      <td>819160b5ba8e3825835d02dc7b56c394</td>
      <td>2020-12-03 01:41:29</td>
      <td>2021-02-05 07:58:10</td>
      <td>12</td>
      <td>64 days 06:16:41</td>
    </tr>
    <tr>
      <th>1859</th>
      <td>f366e28c2dbc162fc593ad137e7639f7</td>
      <td>2020-09-13 20:13:07</td>
      <td>2021-02-05 15:38:00</td>
      <td>6</td>
      <td>144 days 19:24:53</td>
    </tr>
    <tr>
      <th>979</th>
      <td>7ef99db689fc93693173e068f6428f83</td>
      <td>2020-09-12 09:40:40</td>
      <td>2021-02-05 16:17:23</td>
      <td>6</td>
      <td>146 days 06:36:43</td>
    </tr>
    <tr>
      <th>460</th>
      <td>3b2aea159f36c20a012c28fd8a6f3cd4</td>
      <td>2020-06-23 21:33:54</td>
      <td>2021-02-05 23:17:30</td>
      <td>8</td>
      <td>227 days 01:43:36</td>
    </tr>
    <tr>
      <th>117</th>
      <td>10e2f1b840dc70a32e80f7551d679cac</td>
      <td>2019-06-14 13:09:14</td>
      <td>2021-02-06 08:25:05</td>
      <td>12</td>
      <td>602 days 19:15:51</td>
    </tr>
    <tr>
      <th>1165</th>
      <td>974061285c6b459eb174dfbce61e4b1b</td>
      <td>2020-04-03 16:07:24</td>
      <td>2021-02-06 20:56:10</td>
      <td>8</td>
      <td>309 days 04:48:46</td>
    </tr>
    <tr>
      <th>684</th>
      <td>58789e7bb00795f9c12fc7bc9d797e74</td>
      <td>2019-06-26 20:07:27</td>
      <td>2021-02-06 21:29:06</td>
      <td>9</td>
      <td>591 days 01:21:39</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
