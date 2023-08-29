```mermaid

graph TD
    A[Enter Chart Definition] --> B(Preview)
    B --> C{decide}
    C --> D[Keep]
    C --> E[Edit Definition]
    E --> B
    D --> F[Save Image and Code]
    F --> B

```

<!-- https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid -->
<!-- https://mermaid.js.org/intro/n00b-gettingStarted.html -->
<!-- Powerful VSCode Tips And Tricks For Python Development And Design - https://youtu.be/fj2tuTIcUys?t=815 -->

```mermaid

---
title: Simple sample
---
stateDiagram-v2
    [*] --> Still
    Still --> [*]

    Still --> Moving
    Moving --> Still
    Moving --> Crash
    Crash --> [*]


```

```mermaid

---
title: Simple sample
---
stateDiagram-v2
    [*] --> First

    state First {
        [*] --> Second

        state Second {
            [*] --> second
            second --> Third

            state Third {
                [*] --> third
                third --> [*]
            }
        }
    }


```
