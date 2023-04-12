"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[401],{3905:function(e,t,a){a.d(t,{Zo:function(){return p},kt:function(){return m}});var n=a(7294);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,n,o=function(e,t){if(null==e)return{};var a,n,o={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var l=n.createContext({}),h=function(e){var t=n.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},p=function(e){var t=h(e.components);return n.createElement(l.Provider,{value:t},e.children)},d="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var a=e.components,o=e.mdxType,r=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),d=h(a),u=o,m=d["".concat(l,".").concat(u)]||d[u]||c[u]||r;return a?n.createElement(m,i(i({ref:t},p),{},{components:a})):n.createElement(m,i({ref:t},p))}));function m(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=a.length,i=new Array(r);i[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[d]="string"==typeof e?e:o,i[1]=s;for(var h=2;h<r;h++)i[h]=a[h];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}u.displayName="MDXCreateElement"},2465:function(e,t,a){a.r(t),a.d(t,{assets:function(){return l},contentTitle:function(){return i},default:function(){return c},frontMatter:function(){return r},metadata:function(){return s},toc:function(){return h}});var n=a(3117),o=(a(7294),a(3905));const r={sidebar_position:4,id:"resolution",title:"GraphQL Resolution",description:"How data is retrieved and assembled according to the schema and requests"},i="GraphQL Resolution",s={unversionedId:"learn-graphql/guides/resolution",id:"learn-graphql/guides/resolution",title:"GraphQL Resolution",description:"How data is retrieved and assembled according to the schema and requests",source:"@site/docs/learn-graphql/guides/resolution.md",sourceDirName:"learn-graphql/guides",slug:"/learn-graphql/guides/resolution",permalink:"/graphitation/docs/learn-graphql/guides/resolution",draft:!1,editUrl:"https://github.com/microsoft/graphitation/tree/main/website/docs/learn-graphql/guides/resolution.md",tags:[],version:"current",sidebarPosition:4,frontMatter:{sidebar_position:4,id:"resolution",title:"GraphQL Resolution",description:"How data is retrieved and assembled according to the schema and requests"},sidebar:"tutorialSidebar",previous:{title:"Thinking in GraphQL",permalink:"/graphitation/docs/learn-graphql/guides/thinking-in-graphql"},next:{title:"How-To guide for models",permalink:"/graphitation/docs/learn-graphql/howto/howto-models"}},l={},h=[{value:"Get only what you need",id:"get-only-what-you-need",level:2},{value:"Execution",id:"execution",level:2},{value:"\ud83d\udc4e Greedy resolution",id:"-greedy-resolution",level:3},{value:"\ud83d\udc4d Lazy resolution",id:"-lazy-resolution",level:3},{value:"Flexibility for different needs",id:"flexibility-for-different-needs",level:4},{value:"Consistency throughout the schema",id:"consistency-throughout-the-schema",level:4},{value:"Striking the right balance",id:"striking-the-right-balance",level:3},{value:"Models",id:"models",level:2},{value:"Performant data loading",id:"performant-data-loading",level:2},{value:"Basic data loading",id:"basic-data-loading",level:4},{value:"Decoupled batching",id:"decoupled-batching",level:4},{value:"Caching",id:"caching",level:4}],p={toc:h},d="wrapper";function c(e){let{components:t,...a}=e;return(0,o.kt)(d,(0,n.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"graphql-resolution"},"GraphQL Resolution"),(0,o.kt)("p",null,"The role of an execution engine in GraphQL is to convert between underlying services into GraphQL schema types for use in the front-end. We call this \u201cresolution\u201d."),(0,o.kt)("p",null,"It does so by traversing the schema and resolving the fields requested in the query. The executor follows the structure of the query and passes the data returned by each field resolver to its child field resolvers. The executor ensures that only the fields that are requested by the client are resolved, and that the final result matches the shape of the query."),(0,o.kt)("h2",{id:"get-only-what-you-need"},"Get only what you need"),(0,o.kt)("p",null,"A core part of GraphQL is that it allows clients to specify ",(0,o.kt)("strong",{parentName:"p"},"the exact data")," they need from the service. Unlike traditional RESTful APIs, where clients have to make multiple requests or receive more data than they need, GraphQL lets clients define the structure of the data they want and get ",(0,o.kt)("strong",{parentName:"p"},(0,o.kt)("em",{parentName:"strong"},"only")," that data")," in a single request. Notably, this means that the client does not need to pay the price for any business logic required for fields that are not needed by the client. This makes GraphQL APIs more efficient, flexible, and scalable to clients that have such needs."),(0,o.kt)("h2",{id:"execution"},"Execution"),(0,o.kt)("p",null,"To resolve the data of the GraphQL query, we need to define how each field in the schema is fetched from the data source. There are different ways to do this, depending on how we structure our code and how we optimize our performance. In this section, we will explore two examples of how to resolve the data of the query, starting with a naive version that simply returns the full entire response from the root-field, to one that has explicit field resolvers for each field with custom logic."),(0,o.kt)("p",null,"But first, let's quickly cover how the executor will process your query. Let's consider the conversation list UI once more:"),(0,o.kt)("table",null,(0,o.kt)("tr",null,(0,o.kt)("th",null,"Schema"),(0,o.kt)("th",null,"Query")),(0,o.kt)("tr",null,(0,o.kt)("td",null,(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-graphql"},"type Query {\n  conversations: [Conversation]\n}\n\ntype Conversation {\n  title: String\n  lastMessage: String\n  receivedAt: String\n  participants: [Person]\n}\n\ntype Person {\n  avatarURL: String\n}\n"))),(0,o.kt)("td",null,(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-graphql"},"query {\n  conversations {\n    title\n    lastMessage\n    receivedAt\n    participants {\n      avatarUrl\n    }\n  }\n}\n"))))),(0,o.kt)("mermaid",{value:'graph TD\n  A[Query.conversations] --\x3e C["map(Array&lt;Conversation&gt;)"]\n  C --\x3e D[Conversation.title]\n  C --\x3e E[Conversation.lastMessage]\n  C --\x3e F[Conversation.receivedAt]\n  C --\x3e G[Conversation.participants]\n  G --\x3e H["map(Array&lt;Person&gt;)"]\n  H --\x3e I[Person.avatarURL]'}),(0,o.kt)("p",null,"In this case, when we query for conversations, GraphQL will:"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Execute the resolver function for the ",(0,o.kt)("inlineCode",{parentName:"li"},"Query.conversations")," field, which returns an array of ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation")," objects."),(0,o.kt)("li",{parentName:"ol"},"Then, for each individual ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation")," object in the array, GraphQL will execute the resolver function for the ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation.title"),", ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation.lastMessage"),", ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation.receivedAt"),", and ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation.participants")," fields."),(0,o.kt)("li",{parentName:"ol"},"And finally, for each ",(0,o.kt)("inlineCode",{parentName:"li"},"Person")," object in the ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation.participants")," array, GraphQL will execute the resolver function for the ",(0,o.kt)("inlineCode",{parentName:"li"},"Person.avatarURL")," field.")),(0,o.kt)("p",null,"Crucially, each field resolver only resolves exactly that which it is named after. The ",(0,o.kt)("inlineCode",{parentName:"p"},"Query.conversations")," field returns a list of conversations from the data source, it does not transform any values for fields that need custom logic applied, nor does it fetch the person objects for the ",(0,o.kt)("inlineCode",{parentName:"p"},"Conversation.participants")," field."),(0,o.kt)("admonition",{type:"info"},(0,o.kt)("p",{parentName:"admonition"},"For a more details on the functional bits of execution, please refer to ",(0,o.kt)("a",{parentName:"p",href:"https://graphql.org/learn/execution/"},"this graphql.org page"),", or ",(0,o.kt)("a",{parentName:"p",href:"http://spec.graphql.org/October2021/#sec-Execution"},"the spec"),".")),(0,o.kt)("h3",{id:"-greedy-resolution"},"\ud83d\udc4e Greedy resolution"),(0,o.kt)("p",null,"The first example is the simplest one, where we just return the full response from the root-field. This means that we have a single resolver function for the ",(0,o.kt)("inlineCode",{parentName:"p"},"conversations")," field in the ",(0,o.kt)("inlineCode",{parentName:"p"},"Query")," type, and it returns an array of objects that match the shape of the ",(0,o.kt)("inlineCode",{parentName:"p"},"Conversation")," type. We don't need to define any other resolver functions for the nested fields, because GraphQL will ",(0,o.kt)("a",{parentName:"p",href:"#a-note-on-the-default-field-resolver"},"by default")," use the property values of the objects as the field values."),(0,o.kt)("p",null,"For example, if we have a data source that looks like this:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-js"},'const conversations = [\n  {\n    title: "Joshua and Daichi",\n    lastMessage: "You: Thank you!!",\n    receivedAt: "10:29 AM",\n    participants: [\n      {\n        avatarURL: "https://example.com/joshua.jpg",\n      },\n      {\n        avatarURL: "https://example.com/daichi.jpg",\n      },\n    ],\n  },\n  {\n    title: "Kadji Bell",\n    lastMessage: "You: I like the idea, let\u2019s pitch it!",\n    participants: [\n      {\n        avatarURL: "https://example.com/kadji.jpg",\n      },\n    ],\n    receivedAt: "10:02 AM",\n  },\n];\n')),(0,o.kt)("p",null,"Then our resolver function for the conversations field can simply return this array:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-js"},"const resolvers = {\n  Query: {\n    conversations: () => conversations,\n  },\n};\n")),(0,o.kt)("p",null,"This approach is easy to implement, and while it works for trivial queries and data sources, it has some drawbacks. For instance, it does not follow the core idea of GraphQL to ",(0,o.kt)("a",{parentName:"p",href:"#get-only-what-you-need"},"get only what you need"),", which leads to inefficient resource usage and performance issues. If we only want to get the ",(0,o.kt)("inlineCode",{parentName:"p"},"title")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"lastMessage")," fields of each conversation, we still get the participants array with ",(0,o.kt)("em",{parentName:"p"},"all")," their ",(0,o.kt)("inlineCode",{parentName:"p"},"avatarURLs"),". This may seem innocuous in this contrived example, but imagine more complex data sources that require expensive logic to fulfil the participants data, and it can quickly add up."),(0,o.kt)("admonition",{type:"note"},(0,o.kt)("h3",{parentName:"admonition",id:"a-note-on-the-default-field-resolver"},"A note on the default field resolver"),(0,o.kt)("p",{parentName:"admonition"},"The default field resolver is a function that GraphQL uses to resolve the value of a field when no explicit resolver is provided. It works by looking up the property with the same name as the field on the parent object, or calling it as a function if it is one. For example, if we have a field called ",(0,o.kt)("inlineCode",{parentName:"p"},"title")," on a type called ",(0,o.kt)("inlineCode",{parentName:"p"},"Conversation"),", and no resolver for it, the default field resolver will try to return ",(0,o.kt)("inlineCode",{parentName:"p"},"conversation.title")," or call ",(0,o.kt)("inlineCode",{parentName:"p"},"conversation.title()")," if it exists."),(0,o.kt)("p",{parentName:"admonition"},"The following set of resolvers has the same result as the above, but ",(0,o.kt)("em",{parentName:"p"},"without")," relying on the default field resolver:"),(0,o.kt)("pre",{parentName:"admonition"},(0,o.kt)("code",{parentName:"pre",className:"language-js"},"const resolvers = {\n  Query: {\n    conversations: () => conversations,\n  },\n  Conversation: {\n    title: (conversation) => conversation.title,\n    lastMessage: (conversation) => conversation.lastMessage,\n    participants: (conversation) => conversation.participants,\n    receivedAt: (conversation) => conversation.receivedAt,\n  },\n  Person: {\n    avatarURL: (person) => person.avatarURL,\n  },\n};\n"))),(0,o.kt)("h3",{id:"-lazy-resolution"},"\ud83d\udc4d Lazy resolution"),(0,o.kt)("p",null,"The second example is more flexible and efficient than the first one, where we can have explicit field resolvers for each field in the schema. These field resolver functions allow us to define how to derive the field's value from the data source."),(0,o.kt)("p",null,"For example, if the ",(0,o.kt)("inlineCode",{parentName:"p"},"receivedAt")," value would not already be formatted in the data source, we can define a resolver function for this field that calculates its human-readable value from the raw format. Here is how that field resolver function could look like:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-js"},'const resolvers = {\n  Conversation: {\n    // Transform the `conversation.receivedAt` value to HH:MM AM/PM\n    receivedAt: (conversation) => {\n      const date = new Date(conversation.receivedAt);\n      return date.toLocaleTimeString("en-US", {\n        hour: "numeric",\n        hour12: true,\n        minute: "numeric",\n      });\n    },\n  },\n};\n')),(0,o.kt)("p",null,"Similarly, the ",(0,o.kt)("inlineCode",{parentName:"p"},"participants")," value in the data source is more likely to be a list of person IDs, than it is to be a list of full-fledged person objects. In this scenario, we need to issue an extra call to the data source to get the actual data. It should go without saying that we absolutely want this to be done only when the client needs this data, and not fetch it greedily in the ",(0,o.kt)("inlineCode",{parentName:"p"},"Query.conversations")," root-field. Here is how that field resolver function could look like:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-js"},"const resolvers = {\n  Conversation: {\n    participants: (conversation) => getPeopleByIDs(conversation.participantIDs),\n  },\n};\n")),(0,o.kt)("p",null,"Neat."),(0,o.kt)("h4",{id:"flexibility-for-different-needs"},"Flexibility for different needs"),(0,o.kt)("p",null,"We can use this approach to optimize our performance by ",(0,o.kt)("em",{parentName:"p"},"only")," fetching or returning the data that we need for each field. For example, if we only want to get the ",(0,o.kt)("inlineCode",{parentName:"p"},"title"),", ",(0,o.kt)("inlineCode",{parentName:"p"},"lastMessage"),", and ",(0,o.kt)("inlineCode",{parentName:"p"},"receivedAt")," fields of each conversation, we can avoid fetching or returning the participants array with all their ",(0,o.kt)("inlineCode",{parentName:"p"},"avatarURL"),"s."),(0,o.kt)("p",null,"As you have learned in ",(0,o.kt)("a",{parentName:"p",href:"/graphitation/docs/learn-graphql/guides/the-design-of-graphql"},"The Design of GraphQL"),", this flexibility is at the heart of its design for composition of data requirements."),(0,o.kt)("h4",{id:"consistency-throughout-the-schema"},"Consistency throughout the schema"),(0,o.kt)("p",null,"Another benefit of using explicit field resolvers is that they can apply to any field that returns a ",(0,o.kt)("inlineCode",{parentName:"p"},"Conversation")," type, not just the top-level query. This means that you can reuse the same logic and transformations for different queries that also involve conversations. For instance, if you have a ",(0,o.kt)("inlineCode",{parentName:"p"},"Person")," type that has a ",(0,o.kt)("inlineCode",{parentName:"p"},"conversations")," field which returns all the conversations that a user participates in, you can use the same field resolvers as you would use for the ",(0,o.kt)("inlineCode",{parentName:"p"},"Query.conversations")," result. This way, you can avoid inconsistency in your API's results, while staying flexible in the queries it can execute."),(0,o.kt)("p",null,"In this case, only the following schema addition would be necessary to enable the above example:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-graphql"},"type Person {\n  conversations: [Conversation]\n}\n")),(0,o.kt)("p",null,"Plus a field resolver function that does no work other than getting the conversations based on the appropriate context:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-js"},"const resolvers = {\n  Person: {\n    conversations: (person) => getConversationsForPersonById(person.id),\n  },\n  Query: {\n    person: (_, args) => getPerson(args.id),\n  },\n};\n")),(0,o.kt)("p",null,"With that in place, you now have a schema that allows lazy resolution with a query like the following:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-graphql"},'query {\n  person(id: "daichi-fukuda") {\n    conversations {\n      title\n    }\n  }\n}\n')),(0,o.kt)("h3",{id:"striking-the-right-balance"},"Striking the right balance"),(0,o.kt)("p",null,"Using a greedy GraphQL field resolver that does all its work in a single field resolver can ",(0,o.kt)("em",{parentName:"p"},"seem")," like a simple and straightforward way to implement a schema, but ",(0,o.kt)("strong",{parentName:"p"},"it has significant drawbacks")," in terms of resource usage and performance. It results in over-fetching data that is not needed by the client, and wasting time and memory on processing it."),(0,o.kt)("p",null,"In conclusion, lazy field resolvers are ",(0,o.kt)("strong",{parentName:"p"},"the recommended way")," to implement any field that requires some custom logic. This can include scalar fields that need some derivation or transformation, as well as object fields that need to fetch associated data from other sources. Only for fields that are already present in the parent type\u2019s data source, and need no further processing, you can rely on the default field resolver\u2014this usually applies to scalar fields only."),(0,o.kt)("h2",{id:"models"},"Models"),(0,o.kt)("p",null,"In GraphQL execution, there is no need for the GraphQL schema to match the data source. This means that we can design our schema based on the needs of our clients, rather than the structure of our database or API. In fact, very often we will want to hide values that the clients don't need at all or those values from which we derive the field's result."),(0,o.kt)("p",null,"For example, we might have a field in the schema called ",(0,o.kt)("inlineCode",{parentName:"p"},"fullName"),", which concatenates the ",(0,o.kt)("inlineCode",{parentName:"p"},"firstName")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"lastName")," values from our model. We don't need to expose those fields in our schema if they are not useful to our clients, but the field resolver ",(0,o.kt)("em",{parentName:"p"},"does")," need access to the model data for it to be able to do its work."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-graphql"},"type Person {\n  fullName: String!\n\n  # NOTE: These fields do NOT exist in the schema.\n  # firstName: String!\n  # lastName: String!\n}\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-ts"},'const resolvers = {\n  Person: {\n    fullName: (\n      person: DatabaseTablePersonRow,\n    ): SchemaTypes.Person["fullName"] =>\n      `${person.firstName} ${person.lastName}`,\n  },\n};\n')),(0,o.kt)("p",null,"Here, the ",(0,o.kt)("inlineCode",{parentName:"p"},"person")," argument has all the underlying data we need. We call such a source, ",(0,o.kt)("strong",{parentName:"p"},"the model"),". ",(0,o.kt)("em",{parentName:"p"},"Crucially"),", the model type is ",(0,o.kt)("strong",{parentName:"p"},"not")," equal to the schema type. The model type is where the data comes ",(0,o.kt)("em",{parentName:"p"},"from"),", the schema type's field is what the resolver transforms the data ",(0,o.kt)("em",{parentName:"p"},"to"),"."),(0,o.kt)("p",null,"A model can be a raw response from the data source, an intermediate representation, or a full fledged model class instance. A raw data source response is the most basic form of a model. It could be a row from a database table, a document from a database, or a JSON object from an API response."),(0,o.kt)("p",null,"An intermediate representation is a model that has some processing or transformation applied to it, perhaps ahead of time. For example, we might have a model that adds some computed properties during a background synchronization task. Note that this should ",(0,o.kt)("strong",{parentName:"p"},"not")," be transformation to full schema types,"),(0,o.kt)("p",null,"A full fledged model class instance is a model that has methods and behaviors associated with it. For example, we might have a model class that implements validation rules, business logic, or custom methods for manipulating the data."),(0,o.kt)("p",null,"Depending on our use case and preferences, we can choose any of these forms of models for our GraphQL execution. The only requirement is that our resolver functions can access the relevant properties of our models to return the correct values for our schema fields."),(0,o.kt)("admonition",{type:"tip"},(0,o.kt)("p",{parentName:"admonition"},"A good way to think about a model, is that whatever your data source returns ",(0,o.kt)("strong",{parentName:"p"},"is your model"),". In turn, these models are what the resolvers operate on to ",(0,o.kt)("em",{parentName:"p"},"lazily")," map underlying data to the public schema types.")),(0,o.kt)("admonition",{type:"caution"},(0,o.kt)("h4",{parentName:"admonition",id:"a-warning-for-statically-typed-language-users"},"A warning for statically typed language users"),(0,o.kt)("p",{parentName:"admonition"},"For type-safe field resolver implementations, you will typicaly want to generate typings to use in your resolvers. By default, codegen tools will typically emit typings that ",(0,o.kt)("em",{parentName:"p"},"exactly")," match the schema types. What this means is that your field resolver function will be required to return the data for child fields already transformed according to the schema. I.e. this forces you to apply ",(0,o.kt)("a",{parentName:"p",href:"#-greedy-resolution"},"greedy resolution"),". No bueno."),(0,o.kt)("p",{parentName:"admonition"},"You will therefore absolutely want to pick a codegen tool that allows you to specify custom model typings for specific schema types. In the TypeScript space, such tools include:"),(0,o.kt)("ul",{parentName:"admonition"},(0,o.kt)("li",{parentName:"ul"},"Our own ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/microsoft/graphitation/tree/main/packages/cli"},"graphitation supermassive codegen"),", which allows you to annotate your schema definition with the model typings to use."),(0,o.kt)("li",{parentName:"ul"},"The popular ",(0,o.kt)("a",{parentName:"li",href:"https://the-guild.dev/graphql/codegen"},"graphql-codegen")," tool, which ",(0,o.kt)("a",{parentName:"li",href:"https://the-guild.dev/blog/better-type-safety-for-resolvers-with-graphql-codegen"},"allows you to provide configuration")," with schema type to model type mappings."))),(0,o.kt)("h2",{id:"performant-data-loading"},"Performant data loading"),(0,o.kt)("p",null,"Integral to resolution of a graph of connected data, is that a query will end-up containing many entities of the same kind, or perhaps even contain the same entity multiple times. For instance, for each conversation fetch all participants\u2014a classic N+1 problem."),(0,o.kt)("p",null,"These entities might be necessary for unrelated parts of the application, but still, for performance reasons we want to be able to batch that entity data loading. ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/graphql/dataloader"},"DataLoader")," is a utility used to abstract request batching in GraphQL. It allows you to reason about a batch of requests, ",(0,o.kt)("strong",{parentName:"p"},"whithout")," needing to do so in the field resolver functions\u2014keeping them decoupled and without sacrificing the performance of batched data loading."),(0,o.kt)("h4",{id:"basic-data-loading"},"Basic data loading"),(0,o.kt)("p",null,"Let\u2019s look at how DataLoader could be used for the participants in our chat-list example. First we define the DatLoader instance, like so:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-js"},"const personLoader = new DataLoader(async (ids) => {\n  return getPeopleFromServiceByIDs(ids);\n});\n")),(0,o.kt)("p",null,"In essence, DataLoader takes a function that, given a set of IDs (or keys), will return a promise for a set of values."),(0,o.kt)("p",null,"Then, for an individual conversation in the chat-list, we could use the DataLoader instance, like so:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-js"},"const resolvers = {\n  Conversation: {\n    participants: async (conversation) => {\n      return personLoader.loadMany(conversation.participantIds);\n    },\n  },\n};\n")),(0,o.kt)("p",null,"This example isn't all that ground-breaking, as we have the list of IDs in the one field resolver already and can easily load them as a batch. (The only true benefit would be the caching of the people data, allowing for fast retrieval when resolving the same people again elsewhere in the query.)"),(0,o.kt)("h4",{id:"decoupled-batching"},"Decoupled batching"),(0,o.kt)("p",null,"It gets more interesting when we consider that the execution engine will resolve the participants for each conversation in the list ",(0,o.kt)("strong",{parentName:"p"},"in parallel"),". You could imagine it to work something like this pseudo code:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-js"},"Promise.all([\n  Conversation.participants({ ... }),\n  Conversation.participants({ ... })\n])\n")),(0,o.kt)("p",null,"Now, when we pass a single ID (",(0,o.kt)("em",{parentName:"p"},"or set"),") to the DataLoader, we expect a single value (",(0,o.kt)("em",{parentName:"p"},"or respective set"),") to be returned; yet still batch them with the participants of ",(0,o.kt)("em",{parentName:"p"},"all")," other conversations. How this works is that all requests made of a DataLoader during a single tick of the JavaScript runloop, will get batched together and passed to the batch function as a single list."),(0,o.kt)("p",null,"So, given our ",(0,o.kt)("a",{parentName:"p",href:"#%F0%9F%91%8E-greedy-resolution"},"prior example data"),":"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"The execution engine would invoke the ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation.participants")," field resolver twice.",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"Once in a conversation with Joshua and Daichi: ",(0,o.kt)("inlineCode",{parentName:"li"},'personLoader.loadMany(["joshua", "daichi"])')),(0,o.kt)("li",{parentName:"ul"},"And once in a conversation with Kadji: ",(0,o.kt)("inlineCode",{parentName:"li"},'personLoader.loadMany(["kadji"])')))),(0,o.kt)("li",{parentName:"ol"},"The DataLoader instance would then receive the following enqueued IDs as a ",(0,o.kt)("em",{parentName:"li"},"single")," list: ",(0,o.kt)("inlineCode",{parentName:"li"},'["joshua", "daichi", "kadji"]')),(0,o.kt)("li",{parentName:"ol"},"And return the requested people to the 2 invocations of the ",(0,o.kt)("inlineCode",{parentName:"li"},"Conversation.participants")," field resolver for further transforming."),(0,o.kt)("li",{parentName:"ol"},"Finally, the execution engine moves on to the next level of the query, by invoking the ",(0,o.kt)("inlineCode",{parentName:"li"},"Person.avatarURL")," field resolver for each of the 3 people.")),(0,o.kt)("h4",{id:"caching"},"Caching"),(0,o.kt)("p",null,"Additionally, DataLoader provides caching of entities during a single execution pass of an operation. This means that any participants present in all conversations, such as the authenticated user, will only get requested once. But also, if one of those people is requested again later on in the query, DataLoader will simply return it immediately."),(0,o.kt)("admonition",{type:"info"},(0,o.kt)("p",{parentName:"admonition"},"A walkthrough of the DataLoader v1 source code by one of its authors, Lee Byron. While the source has changed since this video was made, it is still a good overview of the rationale of DataLoader and how it works."),(0,o.kt)("a",{href:"https://youtu.be/OQTnXNCDywA",target:"_blank",alt:"DataLoader Source Code Walkthrough"},(0,o.kt)("img",{src:"https://img.youtube.com/vi/OQTnXNCDywA/0.jpg"}))))}c.isMDXComponent=!0}}]);